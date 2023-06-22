package dialect

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

func TestMigrator_FullDataTypeOf(t *testing.T) {
	tests := []struct {
		name     string
		field    *schema.Field
		expr     clause.Expr
		isPanics bool
	}{
		{
			name: "ok",
			field: &schema.Field{
				DataType:          schema.Bool,
				IndirectFieldType: reflect.TypeOf(false),
				Schema: &schema.Schema{
					Name: "SCHEMA",
				},
				Name: "TABLE",
			},
			expr: clause.Expr{
				SQL: "Bool",
			},
		},
		{
			name: "ok not null",
			field: &schema.Field{
				DataType:          schema.Bool,
				IndirectFieldType: reflect.TypeOf(false),
				Schema: &schema.Schema{
					Name: "SCHEMA",
				},
				Name:       "TABLE",
				PrimaryKey: true,
				NotNull:    true,
			},
			expr: clause.Expr{
				SQL: "Bool NOT NULL",
			},
		},
		{
			name: "panic on NOT NULL for non-PrimaryKey column",
			field: &schema.Field{
				DataType:          schema.Bool,
				IndirectFieldType: reflect.TypeOf(false),
				Schema: &schema.Schema{
					Name: "MODEL",
				},
				Name:    "TABLE",
				NotNull: true,
			},
			isPanics: true,
		},
		{
			name: "panic on UNIQUE",
			field: &schema.Field{
				DataType:          schema.Bool,
				IndirectFieldType: reflect.TypeOf(false),
				Schema: &schema.Schema{
					Name: "MODEL",
				},
				Name:   "TABLE",
				Unique: true,
			},
			isPanics: true,
		},
		{
			name: "panic on DEFAULT",
			field: &schema.Field{
				DataType:          schema.Bool,
				IndirectFieldType: reflect.TypeOf(false),
				Schema: &schema.Schema{
					Name: "MODEL",
				},
				Name:            "TABLE",
				HasDefaultValue: true,
				DefaultValue:    "default value",
			},
			isPanics: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := Migrator{
				Migrator: migrator.Migrator{
					Config: migrator.Config{
						Dialector: Dialector{},
					},
				},
				cacheStore: nil,
			}

			if tt.isPanics {
				require.Panics(t, func() {
					m.FullDataTypeOf(tt.field)
				})
				return
			}

			expr := m.FullDataTypeOf(tt.field)
			require.Equal(t, tt.expr, expr)
		})
	}
}
