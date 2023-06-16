package dialect

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"

	ydbDriver "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

type Migrator struct {
	migrator.Migrator

	cacheStore *sync.Map
}

func (m Migrator) FullDataTypeOf(field *schema.Field) (expr clause.Expr) {
	expr.SQL = m.DataTypeOf(field)

	if field.NotNull {
		if !field.PrimaryKey {
			//nolint:godox
			// TODO: remove panic after support NOT NULL for non-PrimaryKey columns
			panic(
				xerrors.WithStacktrace(
					fmt.Errorf("model %s, table %s: not null supported only for PrimaryKey in ydb",
						field.Schema.Name,
						field.Name,
					),
				),
			)
		}
		expr.SQL += " NOT NULL"
	}

	if field.Unique {
		//nolint:godox
		// TODO: implement after support UNIQUE constraint on server side
		panic(
			xerrors.WithStacktrace(
				fmt.Errorf("model %s, table %s: UNIQUE is not supported in ydb",
					field.Schema.Name,
					field.Name,
				),
			),
		)
	}

	if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
		if field.DefaultValueInterface != nil {
			defaultStmt := &gorm.Statement{Vars: []interface{}{field.DefaultValueInterface}}
			m.Dialector.BindVarTo(defaultStmt, defaultStmt, field.DefaultValueInterface)
			expr.SQL += " DEFAULT " + m.Dialector.Explain(defaultStmt.SQL.String(), field.DefaultValueInterface)
		} else if field.DefaultValue != "(-)" {
			expr.SQL += " DEFAULT " + field.DefaultValue
		}
	}

	return expr
}

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
				for _, rel := range stmt.Schema.Relationships.Relations {
					if rel.Field.IgnoreMigration {
						continue
					}
					if constraint := rel.ParseConstraint(); constraint != nil {
						if constraint.Schema == stmt.Schema {
							sql, vars := buildConstraint(constraint)
							createTableSQL += sql + ","
							values = append(values, vars...)
						}
					}
				}
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
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

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

func (m Migrator) HasTable(model interface{}) bool {
	stmt := m.DB.Statement

	var tableName string
	if stmt.Table != "" {
		tableName = stmt.Table
	} else {
		s, err := m.schemaByValue(model)
		if err != nil {
			_ = stmt.AddError(err)
			return false
		}
		tableName = s.Table
	}

	sqlDB, err := m.DB.DB()
	if err != nil {
		_ = stmt.AddError(
			xerrors.WithStacktrace(fmt.Errorf("error getting database/sql driver from gorm: %w", err)),
		)
		return false
	}

	db, err := ydbDriver.Unwrap(sqlDB)
	if err != nil {
		_ = stmt.AddError(xerrors.WithStacktrace(fmt.Errorf("ydb driver unwrap failed: %w", err)))
	}

	exists, err := sugar.IsTableExists(stmt.Context, db.Scheme(), m.fullTableName(tableName))
	if err != nil {
		_ = stmt.AddError(err)
		return false
	}

	return exists
}

// ColumnTypes return columnTypes []gorm.ColumnType and execErr error
func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	columnTypes := make([]gorm.ColumnType, 0)
	execErr := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
		var ct gorm.ColumnType
		for _, f := range stmt.Schema.Fields {
			ct, _, err = Type(f)
			if err != nil {
				return err
			}
			columnTypes = append(columnTypes, ct)
		}

		return
	})

	return columnTypes, execErr
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
		_ = m.DB.Statement.AddError(xerrors.WithStacktrace(errors.New("error conversion to Dialector")))
		return ""
	}

	localPath := path.Join(d.tablePathPrefix, tableName)

	db, err := m.DB.DB()
	if err != nil {
		_ = m.DB.Statement.AddError(xerrors.WithStacktrace(errors.New("error getting DB")))
		return ""
	}

	cc, err := ydbDriver.Unwrap(db)
	if err != nil {
		_ = m.DB.Statement.AddError(xerrors.WithStacktrace(errors.New("error unwrapping db")))
		return ""
	}

	return path.Join(cc.Name(), localPath)
}

func buildConstraint(constraint *schema.Constraint) (sql string, results []interface{}) {
	sql = "CONSTRAINT ? FOREIGN KEY ? REFERENCES ??"
	if constraint.OnDelete != "" {
		sql += " ON DELETE " + constraint.OnDelete
	}

	if constraint.OnUpdate != "" {
		sql += " ON UPDATE " + constraint.OnUpdate
	}

	foreignKeys := make([]interface{}, 0, len(constraint.ForeignKeys))
	for _, field := range constraint.ForeignKeys {
		foreignKeys = append(foreignKeys, clause.Column{Name: field.DBName})
	}

	references := make([]interface{}, 0, len(constraint.References))
	for _, field := range constraint.References {
		references = append(references, clause.Column{Name: field.DBName})
	}

	results = append(results,
		clause.Table{Name: constraint.Name},
		foreignKeys,
		clause.Table{Name: constraint.ReferenceSchema.Table},
		references,
	)

	return
}