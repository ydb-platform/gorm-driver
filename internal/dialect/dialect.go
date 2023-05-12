package dialect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
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

	db       *ydb.Driver
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

func (d *ydbDialect) tableDescription(ctx context.Context, tablePath string) (
	description options.Description, _ error,
) {
	err := d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		description, err = s.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	return description, err
}

func (d *ydbDialect) schemaByValue(model interface{}) (*schema.Schema, error) {
	s, err := schema.Parse(model, &d.cacheStore, d)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return s, nil
}

func (d *ydbDialect) GetTypeAliases(databaseTypeName string) []string {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) createTableQuery(model interface{}) (string, error) {
	s, err := d.schemaByValue(model)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	b := builders.Get()
	defer builders.Put(b)

	b.WriteString("CREATE TABLE `")
	b.WriteString(d.fullTableName(s.Table))
	b.WriteString("` (")

	for i, columnName := range s.DBNames {
		if i > 0 {
			b.WriteString(", ")
		}
		field := s.FieldsByDBName[columnName]
		if !field.IgnoreMigration {
			b.WriteString("\n\t`")
			b.WriteString(columnName)
			b.WriteString("` ")
			b.WriteString(dataTypeOf(field))
		}
	}
	//nolint:godox
	// TODO: if nothing primary keys???
	if len(s.PrimaryFields) > 0 {
		b.WriteString(",\n\tPRIMARY KEY (")
		for i, pk := range s.PrimaryFields {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("`")
			b.WriteString(pk.DBName)
			b.WriteString("`")
		}
		b.WriteString(")")
	}
	for _, idx := range s.ParseIndexes() {
		b.WriteString(",\n\tINDEX `")
		b.WriteString(idx.Name)
		b.WriteString("` GLOBAL ON (")
		for i, field := range idx.Fields {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("`")
			b.WriteString(field.DBName)
			b.WriteString("`")
		}
		b.WriteString(")")
	}
	b.WriteString("\n);")

	return b.String(), nil
}

func (d *ydbDialect) CreateTable(models ...interface{}) error {
	stmt := d.gorm.Statement
	ctx := stmt.Context
	for _, model := range models {
		query, err := d.createTableQuery(model)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		err = d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			return s.ExecuteSchemeQuery(ctx, query)
		}, table.WithIdempotent())
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
	}
	return nil
}

func (d *ydbDialect) dropTableQuery(model interface{}) (_ string, err error) {
	s, err := d.schemaByValue(model)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	b := builders.Get()
	defer builders.Put(b)

	b.WriteString("DROP TABLE `")
	b.WriteString(d.fullTableName(s.Table))
	b.WriteString("`;")

	return b.String(), nil
}

func (d *ydbDialect) DropTable(models ...interface{}) error {
	stmt := d.gorm.Statement
	ctx := stmt.Context
	for _, model := range models {
		if d.HasTable(model) {
			query, err := d.dropTableQuery(model)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
			err = d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				return s.ExecuteSchemeQuery(ctx, query)
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
	s, err := d.schemaByValue(model)
	if err != nil {
		_ = stmt.AddError(xerrors.WithStackTrace(err))
		return false
	}
	exists, err := sugar.IsTableExists(stmt.Context, d.db.Scheme(), d.fullTableName(s.Table))
	if err != nil {
		_ = stmt.AddError(xerrors.WithStackTrace(err))
		return false
	}
	return exists
}

func (d *ydbDialect) RenameTable(oldName, newName interface{}) error {
	//nolint:godox
	// TODO implement me
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

func (d *ydbDialect) TableType(dst interface{}) (gorm.TableType, error) {
	return nil, errors.New("not support")
}

func (d *ydbDialect) addColumnQuery(model interface{}, columnName string) (_ string, err error) {
	s, err := d.schemaByValue(model)
	if err != nil {
		return "", err
	}
	field := s.LookUpField(columnName)

	b := builders.Get()
	defer builders.Put(b)

	b.WriteString("ALTER TABLE `")
	b.WriteString(d.TableName(s.Table))
	b.WriteString("` ADD COLUMN `")
	b.WriteString(columnName)
	b.WriteString("` ")
	b.WriteString(d.DataTypeOf(field))
	b.WriteString(";")

	return b.String(), nil
}

func (d *ydbDialect) AddColumn(model interface{}, columnName string) error {
	query, err := d.addColumnQuery(model, columnName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
	}, table.WithIdempotent())

	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d *ydbDialect) dropColumnQuery(model interface{}, columnName string) (_ string, err error) {
	s, err := d.schemaByValue(model)
	if err != nil {
		return "", err
	}

	b := builders.Get()
	defer builders.Put(b)

	b.WriteString("ALTER TABLE `")
	b.WriteString(d.fullTableName(s.Table))
	b.WriteString("` DROP COLUMN `")
	b.WriteString(columnName)
	b.WriteString("`;")

	return b.String(), nil
}

func (d *ydbDialect) DropColumn(model interface{}, columnName string) error {
	query, err := d.dropColumnQuery(model, columnName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
	}, table.WithIdempotent())

	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (d *ydbDialect) AlterColumn(model interface{}, field string) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) MigrateColumn(model interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) HasColumn(model interface{}, field string) bool {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) RenameColumn(model interface{}, oldName, field string) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) ColumnTypes(model interface{}) (columnTypes []gorm.ColumnType, _ error) {
	s, err := d.schemaByValue(model)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	for _, f := range s.Fields {
		c, _, err := column.Type(f)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		columnTypes = append(columnTypes, c)
	}

	return columnTypes, nil
}

func (d *ydbDialect) CreateView(name string, option gorm.ViewOption) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) DropView(name string) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) CreateConstraint(model interface{}, name string) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) DropConstraint(model interface{}, name string) error {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) HasConstraint(model interface{}, name string) bool {
	//nolint:godox
	// TODO implement me
	panic("implement me")
}

func (d *ydbDialect) createIndexQuery(tableName string, idx *schema.Index) (_ string, err error) {
	b := builders.Get()
	defer builders.Put(b)

	b.WriteString("ALTER TABLE `")
	b.WriteString(d.fullTableName(tableName))
	b.WriteString("` ADD INDEX `")
	b.WriteString(idx.Name)
	b.WriteString("` GLOBAL ON (")

	for i, field := range idx.Fields {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteByte('`')
		b.WriteString(field.DBName)
		b.WriteByte('`')
	}
	b.WriteString(");")

	return b.String(), nil
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

	query, err := d.createIndexQuery(d.fullTableName(s.Table), idx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
	}, table.WithIdempotent())
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func dropIndexQuery(fullTablePath string, indexName string) (_ string, err error) {
	b := builders.Get()
	defer builders.Put(b)

	b.WriteString("ALTER TABLE `")
	b.WriteString(fullTablePath)
	b.WriteString("` DROP INDEX `")
	b.WriteString(indexName)
	b.WriteString("`;")

	return b.String(), nil
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

	query, err := dropIndexQuery(d.TableName(s.Table), indexName)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = d.db.Table().Do(d.gorm.Statement.Context, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
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
	//nolint:godox
	// TODO implement me
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
		var connector driver.Connector
		connector, err = ydb.Connector(d.db,
			ydb.WithTablePathPrefix(d.tablePathPrefix),
			ydb.WithAutoDeclare(),
			ydb.WithNumericArgs(),
		)
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

	return err
}

func (d *ydbDialect) ClauseBuilders() map[string]clause.ClauseBuilder {
	clauseBuilders := map[string]clause.ClauseBuilder{
		"INSERT": d.insertBuilder,
		"SELECT": d.selectBuilder,
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

func (d *ydbDialect) insertBuilder(_ clause.Clause, builder clause.Builder) {
	stmt, ok := builder.(*gorm.Statement)
	if !ok {
		return
	}
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
}

func (d *ydbDialect) selectBuilder(c clause.Clause, builder clause.Builder) {
	stmt, ok := builder.(*gorm.Statement)
	if !ok {
		return
	}
	selectClause, ok := c.Expression.(clause.Select)
	if ok {
		d.selectColumnsBuilder(stmt, selectClause)
	}
	if whereClause, ok := stmt.Clauses["WHERE"].Expression.(clause.Where); ok {
		d.selectWhereBuilder(stmt, whereClause)
	}
}

func (d *ydbDialect) selectColumnsBuilder(stmt *gorm.Statement, c clause.Select) {
	_, _ = stmt.SQL.WriteString("SELECT ")
	if len(c.Columns) > 0 {
		for i, col := range c.Columns {
			if i != 0 {
				_ = stmt.SQL.WriteByte(',')
			}
			_, _ = stmt.SQL.WriteString("`")
			_, _ = stmt.SQL.WriteString(col.Name)
			_, _ = stmt.SQL.WriteString("`")
		}
	} else {
		for i, col := range stmt.Schema.DBNames {
			if i != 0 {
				_ = stmt.SQL.WriteByte(',')
			}
			_, _ = stmt.SQL.WriteString("`")
			_, _ = stmt.SQL.WriteString(col)
			_, _ = stmt.SQL.WriteString("`")
		}
	}
	_, _ = stmt.SQL.WriteString(" FROM `")
	_, _ = stmt.SQL.WriteString(d.fullTableName(stmt.Schema.Table))
	_, _ = stmt.SQL.WriteString("`")
}

func (d *ydbDialect) selectWhereBuilder(stmt *gorm.Statement, c clause.Where) {
	_, _ = stmt.SQL.WriteString(" WHERE ")
	var params []table.ParameterOption
	for _, expr := range c.Exprs {
		expr.Build(stmt)
	}
	for _, param := range params {
		stmt.Vars = append(stmt.Vars, param)
	}
}
