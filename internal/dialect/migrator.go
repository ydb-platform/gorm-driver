package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	ydbDriver "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

// Migrator is wrapper for gorm.Migrator.
type Migrator struct {
	migrator.Migrator

	cacheStore *sync.Map
}

// FullDataTypeOf returns field's db full data type.
func (m Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	expr.SQL = m.DataTypeOf(field)

	if field.NotNull {
		if !field.PrimaryKey {
			//nolint:godox
			// TODO: implement after support NOT NULL for non-PrimaryKey columns
			panic(
				fmt.Sprintf("model %s, table %s: not null supported only for PrimaryKey in ydb",
					field.Schema.Name,
					field.Name,
				),
			)
		}
		expr.SQL += " NOT NULL"
	}

	if field.Unique {
		//nolint:godox
		// TODO: implement after support UNIQUE constraint on server side
		panic(fmt.Sprintf("model %s, table %s: UNIQUE is not supported in ydb", field.Schema.Name, field.Name))
	}

	if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
		//nolint:godox
		// TODO: implement after support DEFAULT in ydb
		panic(fmt.Sprintf("model %s, table %s: DEFAULT is not supported in ydb", field.Schema.Name, field.Name))
	}

	return expr
}

// CreateTable create table in database for values.
func (m Migrator) CreateTable(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})

		if tx.Statement.Context == nil {
			tx.Statement.Context = context.Background()
		}

		tx.Statement.Context = ydbDriver.WithQueryMode(tx.Statement.Context, ydbDriver.SchemeQueryMode)

		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
			var (
				createTableSQL          = "CREATE TABLE ? ("
				values                  = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if !field.IgnoreMigration {
					createTableSQL += "? ?"
					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType ||
						strings.Contains(strings.ToUpper(string(field.DataType)), "PRIMARY KEY")
					values = append(values, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
					createTableSQL += ","
				}
			}

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += "PRIMARY KEY ?,"
				primaryKeys := make([]interface{}, 0, len(stmt.Schema.PrimaryFields))
				for _, field := range stmt.Schema.PrimaryFields {
					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
				}

				values = append(values, primaryKeys)
			}

			for _, idx := range stmt.Schema.ParseIndexes() {
				if m.CreateIndexAfterCreateTable {
					defer func(value interface{}, name string) {
						if err == nil {
							err = tx.Migrator().CreateIndex(value, name)
						}
					}(value, idx.Name)
				} else {
					if idx.Class != "" {
						createTableSQL += idx.Class + " "
					}
					createTableSQL += "INDEX ? GLOBAL ON ?"

					if idx.Comment != "" {
						createTableSQL += fmt.Sprintf(" COMMENT '%s'", idx.Comment)
					}

					if idx.Option != "" {
						createTableSQL += " " + idx.Option
					}

					createTableSQL += ","
					values = append(values,
						clause.Column{Name: idx.Name},
						tx.Migrator().(migrator.BuildIndexOptionsInterface).BuildIndexOptions(idx.Fields, stmt),
					)
				}
			}

			if !m.DB.DisableForeignKeyConstraintWhenMigrating && !m.DB.IgnoreRelationshipsWhenMigrating {
				//nolint:godox
				// TODO: implement after support constraints in ydb
				return xerrors.WithStacktrace(
					fmt.Errorf("model %s: constraints not supported in ydb", stmt.Schema.Name),
				)
			}

			for range stmt.Schema.ParseCheckConstraints() {
				//nolint:godox
				// TODO: implement after support constraints in ydb
				return xerrors.WithStacktrace(
					fmt.Errorf("model %s: constraints not supported in ydb", stmt.Schema.Name),
				)
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")

			createTableSQL += ")"

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprint(tableOption)
			}

			err = tx.Exec(createTableSQL, values...).Error
			return xerrors.WithStacktrace(err)
		}); err != nil {
			return xerrors.WithStacktrace(err)
		}
	}
	return nil
}

// DropTable drop table for values.
func (m Migrator) DropTable(models ...interface{}) error {
	for _, model := range models {
		if m.HasTable(model) {
			tx := m.DB.Session(&gorm.Session{})

			if tx.Statement.Context == nil {
				tx.Statement.Context = context.Background()
			}

			tx.Statement.Context = ydbDriver.WithQueryMode(tx.Statement.Context, ydbDriver.SchemeQueryMode)

			err := m.RunWithValue(
				model,
				func(stmt *gorm.Statement) error {
					return tx.Exec("DROP TABLE ?", m.CurrentTable(stmt)).Error
				},
			)
			if err != nil {
				return xerrors.WithStacktrace(fmt.Errorf("error dropping table: %w", err))
			}
		}
	}
	return nil
}

// HasTable returns table exists or not for value, value could be a struct or string.
func (m Migrator) HasTable(model interface{}) bool {
	stmt := m.DB.Statement

	var tableName string
	if stmt.Table != "" {
		tableName = stmt.Table
	} else {
		s, err := m.schemaByValue(model)
		checkAndAddError(stmt, xerrors.WithStacktrace(err))
		if err != nil {
			return false
		}
		tableName = s.Table
	}

	sqlDB, err := m.DB.DB()
	checkAndAddError(stmt, xerrors.WithStacktrace(fmt.Errorf("error getting database/sql driver from gorm: %w", err)))
	if err != nil {
		return false
	}

	db, err := ydbDriver.Unwrap(sqlDB)
	checkAndAddError(stmt, xerrors.WithStacktrace(fmt.Errorf("ydb driver unwrap failed: %w", err)))
	if err != nil {
		return false
	}

	exists, err := sugar.IsTableExists(stmt.Context, db.Scheme(), m.fullTableName(tableName))
	checkAndAddError(stmt, xerrors.WithStacktrace(err))
	if err != nil {
		return false
	}

	return exists
}

// AddColumn create `name` column for value.
func (m Migrator) AddColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		// avoid using the same name field
		f := stmt.Schema.LookUpField(name)
		if f == nil {
			return xerrors.WithStacktrace(fmt.Errorf("failed to look up field with name: %s", name))
		}

		if !f.IgnoreMigration {
			err := m.DB.WithContext(ydbDriver.WithQueryMode(context.Background(), ydbDriver.SchemeQueryMode)).Exec(
				"ALTER TABLE ? ADD ? ?",
				m.CurrentTable(stmt), clause.Column{Name: f.DBName}, m.DB.Migrator().FullDataTypeOf(f),
			).Error
			return xerrors.WithStacktrace(err)
		}

		return nil
	})
}

// DropColumn drop value's `name` column.
func (m Migrator) DropColumn(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(name); field != nil {
			name = field.DBName
		}

		err := m.DB.WithContext(ydbDriver.WithQueryMode(context.Background(), ydbDriver.SchemeQueryMode)).Exec(
			"ALTER TABLE ? DROP COLUMN ?", m.CurrentTable(stmt), clause.Column{Name: name},
		).Error
		return xerrors.WithStacktrace(err)
	})
}

// AlterColumn alter value's `field` column type based on schema definition.
func (m Migrator) AlterColumn(_ interface{}, field string) error {
	return xerrors.WithStacktrace(fmt.Errorf("field `%s`: alter column not supported", field))
}

// ColumnTypes return columnTypes []gorm.ColumnType and execErr error.
func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	s, err := m.schemaByValue(value)
	if err != nil {
		return nil, xerrors.WithStacktrace(err)
	}
	tableName := s.Table

	columnTypes := make([]gorm.ColumnType, 0)
	execErr := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
		if stmt.Context == nil {
			stmt.Context = context.Background()
		}

		var db *sql.DB
		db, err = m.DB.DB()
		if err != nil {
			return xerrors.WithStacktrace(err)
		}

		var cc *ydbDriver.Driver
		cc, err = ydbDriver.Unwrap(db)
		if err != nil {
			return xerrors.WithStacktrace(err)
		}

		pt := m.fullTableName(tableName)

		var desc options.Description
		err = cc.Table().Do(stmt.Context, func(ctx context.Context, s table.Session) (err error) {
			desc, err = s.DescribeTable(ctx, pt)
			return xerrors.WithStacktrace(err)
		}, table.WithIdempotent())
		if err != nil {
			return xerrors.WithStacktrace(fmt.Errorf("describe '%s' failed: %w", pt, err))
		}

		var ct gorm.ColumnType
	field:
		for _, f := range stmt.Schema.Fields {
			for _, column := range desc.Columns {
				if f.DBName == column.Name {
					ct, _, err = TypeByYdbType(f, column.Type)
					if err != nil {
						return xerrors.WithStacktrace(err)
					}

					columnTypes = append(columnTypes, ct)

					continue field
				}
			}
		}

		return
	})

	return columnTypes, xerrors.WithStacktrace(execErr)
}

func (m Migrator) schemaByValue(model interface{}) (*schema.Schema, error) {
	s, err := schema.Parse(model, m.cacheStore, m.DB.NamingStrategy)
	if err != nil {
		return nil, xerrors.WithStacktrace(fmt.Errorf("error parsing schema: %w", err))
	}
	return s, nil
}

func (m Migrator) fullTableName(tableName string) string {
	d, ok := m.Dialector.(Dialector)
	if !ok {
		checkAndAddError(m.DB.Statement, xerrors.WithStacktrace(errors.New("error conversion to Dialector")))
		return ""
	}

	localPath := path.Join(d.tablePathPrefix, tableName)

	db, err := m.DB.DB()
	if err != nil {
		checkAndAddError(m.DB.Statement, xerrors.WithStacktrace(errors.New("error getting DB")))
		return ""
	}

	cc, err := ydbDriver.Unwrap(db)
	if err != nil {
		checkAndAddError(m.DB.Statement, xerrors.WithStacktrace(errors.New("error unwrapping db")))
		return ""
	}

	return path.Join(cc.Name(), localPath)
}
