package dialect

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/builders"
	"github.com/ydb-platform/gorm-driver/internal/column"
	"github.com/ydb-platform/gorm-driver/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type Option func(d *ydbDialect)

func With(opts ...ydb.Option) Option {
	return func(d *ydbDialect) {
		d.opts = append(d.opts, opts...)
	}
}

func WithTablePathPrefix(tablePathPrefix string) Option {
	return func(d *ydbDialect) {
		d.tablePathPrefix = tablePathPrefix
	}
}

func New(dsn string, opts ...Option) (d *ydbDialect) {
	d = &ydbDialect{
		dsn: dsn,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

var (
	_ gorm.Dialector = (*ydbDialect)(nil)
	_ gorm.Migrator  = (*ydbDialect)(nil)
	_ schema.Namer   = (*ydbDialect)(nil)
)

type ydbDialect struct {
	schema.NamingStrategy
	tablePathPrefix string

	dsn  string
	opts []ydb.Option

	cacheStore sync.Map

	db       ydb.Connection
	gorm     *gorm.DB
	migrator migrator.Migrator
}

func (d *ydbDialect) fullTableName(tableName string) string {
	return path.Join(d.tablePathPrefix, tableName)
}

func (d *ydbDialect) AutoMigrate(models ...interface{}) error {
	return d.migrator.AutoMigrate(models...)
}

func (d *ydbDialect) CurrentDatabase() string {
	return d.db.Name()
}

func (d *ydbDialect) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	expr.SQL = dataTypeOf(field)

	if field.NotNull {
		expr.SQL += " NOT NULL"
	}

	return
}

func (d *ydbDialect) tableDescription(ctx context.Context, tablePath string) (description options.Description, _ error) {
	err := d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		description, err = s.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	return description, err
}

func (d *ydbDialect) schemaByValue(model interface{}) (*schema.Schema, error) {
	schema, err := schema.Parse(model, &d.cacheStore, d)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return schema, nil
}

func (d *ydbDialect) GetTypeAliases(databaseTypeName string) []string {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) createTableQuery(model interface{}) (string, error) {
	schema, err := d.schemaByValue(model)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	sql := builders.Get()
	defer builders.Put(sql)

	sql.WriteString("CREATE TABLE `")
	sql.WriteString(d.fullTableName(schema.Table))
	sql.WriteString("` (")

	for i, columnName := range schema.DBNames {
		if i > 0 {
			sql.WriteString(", ")
		}
		field := schema.FieldsByDBName[columnName]
		if !field.IgnoreMigration {
			sql.WriteString("\n\t`")
			sql.WriteString(columnName)
			sql.WriteString("` ")
			sql.WriteString(dataTypeOf(field))
		}
	}
	// TODO: if nothing primary keys???
	if len(schema.PrimaryFields) > 0 {
		sql.WriteString(",\n\tPRIMARY KEY (")
		for i, pk := range schema.PrimaryFields {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString("`")
			sql.WriteString(pk.DBName)
			sql.WriteString("`")
		}
		sql.WriteString(")")
	}
	for _, idx := range schema.ParseIndexes() {
		sql.WriteString(",\n\tINDEX `")
		sql.WriteString(idx.Name)
		sql.WriteString("` GLOBAL ON (")
		for i, field := range idx.Fields {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString("`")
			sql.WriteString(field.DBName)
			sql.WriteString("`")
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n);")

	return sql.String(), nil
}

func (d *ydbDialect) CreateTable(models ...interface{}) error {
	stmt := d.gorm.Statement
	ctx := stmt.Context
	for _, model := range models {
		sql, err := d.createTableQuery(model)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		err = d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			return s.ExecuteSchemeQuery(ctx, sql)
		}, table.WithIdempotent())
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}
	return nil
}

func (d *ydbDialect) dropTableQuery(model interface{}) (_ string, err error) {
	schema, err := d.schemaByValue(model)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	sql := builders.Get()
	defer builders.Put(sql)

	sql.WriteString("DROP TABLE `")
	sql.WriteString(d.fullTableName(schema.Table))
	sql.WriteString("`;")

	return sql.String(), nil
}

func (d *ydbDialect) DropTable(models ...interface{}) error {
	stmt := d.gorm.Statement
	ctx := stmt.Context
	for _, model := range models {
		if d.HasTable(model) {
			sql, err := d.dropTableQuery(model)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
			err = d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				return s.ExecuteSchemeQuery(ctx, sql)
			}, table.WithIdempotent())
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
		}
	}
	return nil
}

func (d *ydbDialect) HasTable(model interface{}) bool {
	stmt := d.gorm.Statement
	schema, err := d.schemaByValue(model)
	if err != nil {
		_ = stmt.AddError(xerrors.WithStackTrace(err))
		return false
	}
	exists, err := sugar.IsTableExists(stmt.Context, d.db.Scheme(), d.fullTableName(schema.Table))
	if err != nil {
		_ = stmt.AddError(xerrors.WithStackTrace(err))
		return false
	}
	return exists
}

func (d *ydbDialect) RenameTable(oldName, newName interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) listTables(absPath string) (tableList []string, err error) {
	stmt := d.gorm.Statement
	dir, err := d.db.Scheme().ListDirectory(stmt.Context, absPath)
	if err != nil {
		return nil, err
	}
	for _, e := range dir.Children {
		switch e.Type {
		case scheme.EntryTable:
			tableList = append(tableList, e.Name)
		case scheme.EntryDirectory:
			childTables, err := d.listTables(path.Join(absPath, e.Name))
			if err != nil {
				return nil, err
			}
			tableList = append(tableList, childTables...)
		}
	}
	return tableList, nil
}

func (d *ydbDialect) GetTables() (tableList []string, err error) {
	return d.listTables(d.tablePathPrefix)
}

func (d *ydbDialect) addColumnQuery(model interface{}, columnName string) (_ string, err error) {
	schema, err := d.schemaByValue(model)
	if err != nil {
		return "", err
	}
	field := schema.LookUpField(columnName)

	sql := builders.Get()
	defer builders.Put(sql)

	sql.WriteString("ALTER TABLE `")
	sql.WriteString(d.TableName(schema.Table))
	sql.WriteString("` ADD COLUMN `")
	sql.WriteString(columnName)
	sql.WriteString("` ")
	sql.WriteString(d.DataTypeOf(field))
	sql.WriteString(";")

	return sql.String(), nil
}

func (d *ydbDialect) AddColumn(model interface{}, columnName string) error {
	sql, err := d.addColumnQuery(model, columnName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, sql)
	}, table.WithIdempotent())

	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d *ydbDialect) dropColumnQuery(model interface{}, columnName string) (_ string, err error) {
	schema, err := d.schemaByValue(model)
	if err != nil {
		return "", err
	}

	sql := builders.Get()
	defer builders.Put(sql)

	sql.WriteString("ALTER TABLE `")
	sql.WriteString(d.fullTableName(schema.Table))
	sql.WriteString("` DROP COLUMN `")
	sql.WriteString(columnName)
	sql.WriteString("`;")

	return sql.String(), nil
}

func (d *ydbDialect) DropColumn(model interface{}, columnName string) error {
	sql, err := d.dropColumnQuery(model, columnName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, sql)
	}, table.WithIdempotent())

	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d *ydbDialect) AlterColumn(model interface{}, field string) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) MigrateColumn(model interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) HasColumn(model interface{}, field string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) RenameColumn(model interface{}, oldName, field string) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) ColumnTypes(model interface{}) (columnTypes []gorm.ColumnType, _ error) {
	schema, err := d.schemaByValue(model)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	for _, f := range schema.Fields {
		c, _, err := column.Type(f)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		columnTypes = append(columnTypes, c)
	}

	return columnTypes, nil
}

func (d *ydbDialect) CreateView(name string, option gorm.ViewOption) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) DropView(name string) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) CreateConstraint(model interface{}, name string) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) DropConstraint(model interface{}, name string) error {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) HasConstraint(model interface{}, name string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *ydbDialect) createIndexQuery(tableName string, idx *schema.Index) (_ string, err error) {
	sql := builders.Get()
	defer builders.Put(sql)

	sql.WriteString("ALTER TABLE `")
	sql.WriteString(d.fullTableName(tableName))
	sql.WriteString("` ADD INDEX `")
	sql.WriteString(idx.Name)
	sql.WriteString("` GLOBAL ON (")

	for i, field := range idx.Fields {
		if i != 0 {
			sql.WriteString(", ")
		}
		sql.WriteByte('`')
		sql.WriteString(field.DBName)
		sql.WriteByte('`')
	}
	sql.WriteString(");")

	return sql.String(), nil
}

func (d *ydbDialect) CreateIndex(model interface{}, indexName string) error {
	s, err := d.schemaByValue(model)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	idx := s.LookIndex(indexName)
	if idx == nil {
		return xerrors.WithStackTrace(errIndexNotFount)
	}

	sql, err := d.createIndexQuery(d.fullTableName(s.Table), idx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, sql)
	}, table.WithIdempotent())
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func dropIndexQuery(fullTablePath string, indexName string) (_ string, err error) {
	sql := builders.Get()
	defer builders.Put(sql)

	sql.WriteString("ALTER TABLE `")
	sql.WriteString(fullTablePath)
	sql.WriteString("` DROP INDEX `")
	sql.WriteString(indexName)
	sql.WriteString("`;")

	return sql.String(), nil
}

func (d *ydbDialect) DropIndex(model interface{}, indexName string) error {
	s, err := d.schemaByValue(model)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	idx := s.LookIndex(indexName)
	if idx == nil {
		return xerrors.WithStackTrace(errIndexNotFount)
	}

	sql, err := dropIndexQuery(d.TableName(s.Table), indexName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, sql)
	}, table.WithIdempotent())
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d *ydbDialect) HasIndex(model interface{}, indexName string) bool {
	stmt := d.gorm.Statement
	ctx := stmt.Context
	s, err := d.schemaByValue(model)
	if err != nil {
		_ = stmt.AddError(xerrors.WithStackTrace(err))
		return false
	}
	description, err := d.tableDescription(ctx, d.TableName(s.Table))
	if err != nil {
		_ = stmt.AddError(xerrors.WithStackTrace(err))
		return false
	}
	for _, idx := range description.Indexes {
		if idx.Name == indexName {
			return true
		}
	}
	return false
}

func (d *ydbDialect) RenameIndex(model interface{}, oldName, newName string) error {
	s, err := d.schemaByValue(model)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	idx := s.LookIndex(oldName)
	if idx == nil {
		return xerrors.WithStackTrace(errIndexNotFount)
	}

	create, err := d.createIndexQuery(s.Table, idx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	drop, err := dropIndexQuery(d.TableName(s.Table), oldName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, create+"\n"+drop)
	}, table.WithIdempotent())
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d *ydbDialect) GetIndexes(model interface{}) ([]gorm.Index, error) {
	//TODO implement me
	panic("implement me")
}

var (
	// CreateClauses create clauses
	CreateClauses = []string{"INSERT"}
	// QueryClauses query clauses
	QueryClauses = []string{"SELECT"}
	// UpdateClauses update clauses
	UpdateClauses = []string{"UPDATE", "SET", "WHERE", "ORDER BY", "LIMIT"}
	// DeleteClauses delete clauses
	DeleteClauses = []string{"DELETE", "FROM", "WHERE", "ORDER BY", "LIMIT"}
)

func (d *ydbDialect) Name() string {
	return "ydb"
}

// NowFunc return now func
func (d *ydbDialect) NowFunc(n int) func() time.Time {
	return func() time.Time {
		round := time.Second / time.Duration(math.Pow10(n))
		return time.Now().Round(round)
	}
}

func (d *ydbDialect) Apply(config *gorm.Config) error {
	return nil
}

func (d *ydbDialect) Initialize(db *gorm.DB) (err error) {
	var ctx context.Context
	if db.Statement != nil && db.Statement.Context != nil {
		ctx = db.Statement.Context
	} else {
		ctx = context.Background()
	}

	if d.db == nil {
		d.db, err = ydb.Open(ctx, d.dsn, d.opts...)
		if err != nil {
			return err
		}
		d.tablePathPrefix = path.Join(d.db.Name(), d.tablePathPrefix)
	}

	if db.ConnPool == nil {
		connector, err := ydb.Connector(d.db)
		if err != nil {
			return err
		}
		db.ConnPool = sql.OpenDB(connector)
	}

	if d.gorm == nil {
		d.gorm = db
		d.migrator.Config.DB = db
		d.migrator.Config.Dialector = d
	}

	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: CreateClauses,
		QueryClauses:  QueryClauses,
		UpdateClauses: UpdateClauses,
		DeleteClauses: DeleteClauses,
	})

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	return
}

func (d *ydbDialect) ClauseBuilders() map[string]clause.ClauseBuilder {
	clauseBuilders := map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if stmt, ok := builder.(*gorm.Statement); ok {
				if valuesClause, ok := stmt.Clauses["VALUES"].Expression.(clause.Values); ok {
					// make params arg
					rows := make([]types.Value, len(valuesClause.Values))
					for i, row := range valuesClause.Values {
						fields := make([]types.StructValueOption, len(row))
						for j, v := range row {
							fields[j] = types.StructFieldValue(valuesClause.Columns[j].Name, column.Value(v))
						}
						rows[i] = types.StructValue(fields...)
					}
					listValue := types.ListValue(rows...)

					stmt.Vars = []interface{}{table.ValueParam("$values", listValue)}

					// write DECLARE for param $values
					_, _ = stmt.WriteString("DECLARE $values AS ")
					_, _ = stmt.WriteString(listValue.Type().Yql())
					_, _ = stmt.WriteString("; ")
					// write INSERT statement
					_, _ = stmt.WriteString("UPSERT INTO `")
					_, _ = stmt.WriteString(d.fullTableName(stmt.Table))
					_, _ = stmt.WriteString("` (")
					for i, col := range valuesClause.Columns {
						if i != 0 {
							_ = stmt.WriteByte(',')
						}
						_, _ = stmt.WriteString("`")
						_, _ = stmt.WriteString(col.Name)
						_, _ = stmt.WriteString("`")
					}
					_, _ = stmt.WriteString(") SELECT ")
					for i, col := range valuesClause.Columns {
						if i != 0 {
							_ = stmt.WriteByte(',')
						}
						_, _ = stmt.WriteString("`")
						_, _ = stmt.WriteString(col.Name)
						_, _ = stmt.WriteString("`")
					}
					_, _ = stmt.WriteString(" FROM AS_TABLE($values);")
				}
				return
			}
		},
		"SELECT": func(c clause.Clause, builder clause.Builder) {
			if stmt, ok := builder.(*gorm.Statement); ok {
				sql := builders.Get()
				defer builders.Put(sql)
				if selectClause, ok := c.Expression.(clause.Select); ok {
					_, _ = sql.WriteString("SELECT ")
					if len(selectClause.Columns) > 0 {
						for i, col := range selectClause.Columns {
							if i != 0 {
								_ = sql.WriteByte(',')
							}
							_, _ = sql.WriteString("`")
							_, _ = sql.WriteString(col.Name)
							_, _ = sql.WriteString("`")
						}
					} else {
						for i, col := range stmt.Schema.DBNames {
							if i != 0 {
								_ = sql.WriteByte(',')
							}
							_, _ = sql.WriteString("`")
							_, _ = sql.WriteString(col)
							_, _ = sql.WriteString("`")
						}
					}
					_, _ = sql.WriteString(" FROM `")
					_, _ = sql.WriteString(d.fullTableName(stmt.Schema.Table))
					_, _ = sql.WriteString("`")
				}
				if whereClause, ok := stmt.Clauses["WHERE"].Expression.(clause.Where); ok {
					_, _ = sql.WriteString(" WHERE ")
					var params []table.ParameterOption
					writeExpression := func(column interface{}, op string) (columnName string) {
						switch c := column.(type) {
						case clause.Column:
							columnName = c.Name
						case string:
							columnName = c
						default:
							panic(fmt.Sprintf("unkown type of %+v", c))
						}
						_, _ = sql.WriteString("`")
						_, _ = sql.WriteString(columnName)
						_, _ = sql.WriteString("`")
						_, _ = sql.WriteString(op)
						columnName = "$arg" + strconv.Itoa(len(params))
						_, _ = sql.WriteString(columnName)
						return columnName
					}
					for _, expr := range whereClause.Exprs {
						switch t := expr.(type) {
						case clause.IN:
							values := make([]types.Value, len(t.Values))
							for i, in := range t.Values {
								values[i] = column.Value(in)
							}
							params = append(params, table.ValueParam(writeExpression(t.Column, " IN "), types.ListValue(values...)))
						case clause.Like:
							params = append(params, table.ValueParam(writeExpression(t.Column, " LIKE "), column.Value(t.Value)))
						case clause.Eq:
							params = append(params, table.ValueParam(writeExpression(t.Column, "="), column.Value(t.Value)))
						case clause.Neq:
							params = append(params, table.ValueParam(writeExpression(t.Column, "!="), column.Value(t.Value)))
						case clause.Gt:
							params = append(params, table.ValueParam(writeExpression(t.Column, ">"), column.Value(t.Value)))
						case clause.Gte:
							params = append(params, table.ValueParam(writeExpression(t.Column, ">="), column.Value(t.Value)))
						case clause.Lt:
							params = append(params, table.ValueParam(writeExpression(t.Column, "<"), column.Value(t.Value)))
						case clause.Lte:
							params = append(params, table.ValueParam(writeExpression(t.Column, "<="), column.Value(t.Value)))
						default:
							panic(fmt.Sprintf("unkown type of %+v", expr))
						}
					}
					declares, err := sugar.GenerateDeclareSection(params)
					if err != nil {
						_ = stmt.AddError(err)
						return
					}
					_, _ = stmt.WriteString(strings.ReplaceAll(declares, "\n", " "))
					for _, param := range params {
						stmt.Vars = append(stmt.Vars, param)
					}
				}
				_, _ = stmt.WriteString(sql.String())
				return
			}
		},
	}

	return clauseBuilders
}

func (d *ydbDialect) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (d *ydbDialect) Migrator(db *gorm.DB) gorm.Migrator {
	return d
}

func (d *ydbDialect) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	_ = writer.WriteByte('$')
	_, _ = writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

func (d *ydbDialect) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
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
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
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

func (d *ydbDialect) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (d *ydbDialect) SavePoint(tx *gorm.DB, name string) error {
	return gorm.ErrUnsupportedDriver
}

func (d *ydbDialect) RollbackTo(tx *gorm.DB, name string) error {
	return gorm.ErrUnsupportedDriver
}

func dataTypeOf(f *schema.Field) string {
	t, _, err := column.Type(f)
	if err != nil {
		panic(err)
	}
	return t.DatabaseTypeName()
}

func (d *ydbDialect) DataTypeOf(f *schema.Field) string {
	return dataTypeOf(f)
}
