package dialect

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

// Option is option for Dialector New constructor.
type Option func(d *Dialector)

// With apply ydb.Option to Dialector.
func With(opts ...ydb.Option) Option {
	return func(d *Dialector) {
		d.opts = append(d.opts, opts...)
	}
}

// WithTablePathPrefix apply table path prefix to Dialector.
func WithTablePathPrefix(tablePathPrefix string) Option {
	return func(d *Dialector) {
		d.tablePathPrefix = tablePathPrefix
	}
}

// WithMaxOpenConns apply max open conns to Dialector.
func WithMaxOpenConns(maxOpenConns int) Option {
	return func(d *Dialector) {
		d.maxOpenConns = maxOpenConns
	}
}

// WithMaxIdleConns apply max idle conns to Dialector.
func WithMaxIdleConns(maxIdleConns int) Option {
	return func(d *Dialector) {
		d.maxIdleConns = maxIdleConns
	}
}

// WithConnMaxIdleTime apply max idle time to Dialector.
func WithConnMaxIdleTime(connMaxIdleTime time.Duration) Option {
	return func(d *Dialector) {
		d.connMaxIdleTime = connMaxIdleTime
	}
}

// Dialector is implementation of gorm.Dialector.
type Dialector struct {
	DSN  string
	Conn gorm.ConnPool

	opts            []ydb.Option
	tablePathPrefix string
	maxOpenConns    int
	maxIdleConns    int
	connMaxIdleTime time.Duration
}

// New is constructor for Dialector.
func New(dsn string, opts ...Option) *Dialector {
	d := &Dialector{
		DSN: dsn,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}

	return d
}

func (d Dialector) Name() string {
	return "ydb"
}

func (d Dialector) Initialize(db *gorm.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		cc, err := ydb.Open(ctx, d.DSN, d.opts...)
		if err != nil {
			return xerrors.WithStacktrace(fmt.Errorf("connect error: %w", err))
		}

		c, err := ydb.Connector(cc,
			ydb.WithTablePathPrefix(d.tablePathPrefix),
			ydb.WithAutoDeclare(),
			ydb.WithNumericArgs(),
		)
		if err != nil {
			return xerrors.WithStacktrace(fmt.Errorf("create connector error: %w", err))
		}

		conn := sql.OpenDB(c)

		conn.SetMaxOpenConns(d.maxOpenConns)
		conn.SetMaxIdleConns(d.maxIdleConns)
		conn.SetConnMaxIdleTime(d.connMaxIdleTime)

		db.ConnPool = conn

		db.DisableForeignKeyConstraintWhenMigrating = true
		db.IgnoreRelationshipsWhenMigrating = true
	}

	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses:        []string{"INSERT", "VALUES"},
		UpdateClauses:        []string{"UPDATE", "SET", "WHERE"},
		DeleteClauses:        []string{"DELETE", "FROM", "WHERE"},
		LastInsertIDReversed: true,
	})

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	return nil
}

func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			insert, ok := c.Expression.(clause.Insert)
			if !ok {
				c.Build(builder)

				return
			}

			stmt, ok := builder.(*gorm.Statement)
			if !ok {
				c.Build(builder)

				return
			}

			_, err := stmt.WriteString("UPSERT ")
			checkAndAddError(stmt, err)

			if insert.Modifier != "" {
				_, err = stmt.WriteString(insert.Modifier)
				checkAndAddError(stmt, err)

				err = stmt.WriteByte(' ')
				checkAndAddError(stmt, err)
			}

			_, err = stmt.WriteString("INTO ")
			checkAndAddError(stmt, err)

			if insert.Table.Name == "" {
				stmt.WriteQuoted(stmt.Table)
			} else {
				stmt.WriteQuoted(insert.Table)
			}
		},
	}
}

func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	m := migrator.Migrator{
		Config: migrator.Config{
			DB:        db,
			Dialector: d,
		},
	}

	return Migrator{
		Migrator:   m,
		cacheStore: &sync.Map{},
	}
}

func (d Dialector) DataTypeOf(field *schema.Field) string {
	t, _, err := parseField(field)
	if err != nil {
		panic(fmt.Errorf("error getting field (model %s, field %s) type: %w", field.Schema.Name, field.Name, err))
	}

	return t.DatabaseTypeName()
}

func (d Dialector) DefaultValueOf(_ *schema.Field) clause.Expression {
	//nolint:godox
	// TODO: implement after support DEFAULT in ydb
	panic("DEFAULT in not supported in ydb")
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, _ interface{}) {
	err := writer.WriteByte('$')
	checkAndAddError(stmt, err)

	_, err = writer.WriteString(strconv.Itoa(len(stmt.Vars)))
	checkAndAddError(stmt, err)
}

func (d Dialector) QuoteTo(writer clause.Writer, s string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
		backticksCount          int8
	)

	for _, v := range []byte(s) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				_, _ = writer.WriteString("``")
				backticksCount += 2
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				_ = writer.WriteByte('`')
				backticksCount++
			}
			_ = writer.WriteByte(v)

			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				_ = writer.WriteByte('`')
				backticksCount++
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick--
				}
			}

			for ; continuousBacktick > 0; continuousBacktick-- {
				_, _ = writer.WriteString("``")
				backticksCount += 2
			}

			_ = writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		_, _ = writer.WriteString("``")
		backticksCount += 2
	}

	if backticksCount%2 != 0 {
		_ = writer.WriteByte('`')
	}
}

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}
