package dialect

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbDriver "github.com/ydb-platform/ydb-go-sdk/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

type Option func(d *Dialector)

func With(opts ...ydbDriver.Option) Option {
	return func(d *Dialector) {
		d.opts = append(d.opts, opts...)
	}
}

func WithTablePathPrefix(tablePathPrefix string) Option {
	return func(d *Dialector) {
		d.tablePathPrefix = tablePathPrefix
	}
}

func WithMaxOpenConns(maxOpenConns int) Option {
	return func(d *Dialector) {
		d.maxOpenConns = maxOpenConns
	}
}

func WithMaxIdleConns(maxIdleConns int) Option {
	return func(d *Dialector) {
		d.maxIdleConns = maxIdleConns
	}
}

func WithConnMaxIdleTime(connMaxIdleTime time.Duration) Option {
	return func(d *Dialector) {
		d.connMaxIdleTime = connMaxIdleTime
	}
}

type Dialector struct {
	DSN  string
	Conn gorm.ConnPool

	opts            []ydbDriver.Option
	tablePathPrefix string
	maxOpenConns    int
	maxIdleConns    int
	connMaxIdleTime time.Duration
}

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
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d Dialector) Initialize(db *gorm.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		cc, err := ydbDriver.Open(ctx,
			d.DSN,
			environ.WithEnvironCredentials(ctx),
		)
		if err != nil {
			return xerrors.WithStacktrace(fmt.Errorf("connect error: %w", err))
		}

		d.tablePathPrefix = path.Join(cc.Name(), d.tablePathPrefix)

		c, err := ydbDriver.Connector(cc,
			ydbDriver.WithTablePathPrefix(d.tablePathPrefix),
			ydbDriver.WithAutoDeclare(),
			ydbDriver.WithNumericArgs(),
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
			d.checkAndAddError(stmt, err)

			if insert.Modifier != "" {
				_, err = stmt.WriteString(insert.Modifier)
				d.checkAndAddError(stmt, err)

				err = stmt.WriteByte(' ')
				d.checkAndAddError(stmt, err)
			}

			_, err = stmt.WriteString("INTO ")
			d.checkAndAddError(stmt, err)

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
	t, _, err := Type(field)
	if err != nil {
		panic(
			xerrors.WithStacktrace(
				fmt.Errorf("error getting field (model %s, field %s) type: %w",
					field.Schema.Name,
					field.Name,
					err,
				),
			),
		)
	}
	return t.DatabaseTypeName()
}

func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	err := writer.WriteByte('$')
	d.checkAndAddError(stmt, err)

	_, err = writer.WriteString(strconv.Itoa(len(stmt.Vars)))
	d.checkAndAddError(stmt, err)
}

func (d Dialector) QuoteTo(writer clause.Writer, s string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(s) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				_, _ = writer.WriteString("``")
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				_ = writer.WriteByte('`')
			}
			_ = writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				_ = writer.WriteByte('`')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick--
				}
			}

			for ; continuousBacktick > 0; continuousBacktick-- {
				_, _ = writer.WriteString("``")
			}

			_ = writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		_, _ = writer.WriteString("``")
	}
	_ = writer.WriteByte('`')
}

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (d Dialector) checkAndAddError(stmt *gorm.Statement, err error) {
	if err != nil {
		_ = stmt.AddError(err)
	}
}
