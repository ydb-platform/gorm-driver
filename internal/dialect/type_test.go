package dialect

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

func TestTypeByYdbType(t *testing.T) {
	tests := []struct {
		field     *schema.Field
		typesType types.Type
		nullable  bool
		options   []TypeByYdbTypeOption
	}{
		{
			field: &schema.Field{
				DBName:     uuid.New().String(),
				DataType:   schema.Bool,
				PrimaryKey: false,
			},
			typesType: types.TypeBool,
		},
		{
			field: &schema.Field{
				DBName:     uuid.New().String(),
				DataType:   schema.Bool,
				PrimaryKey: true,
			},
			typesType: types.TypeBool,
		},
		{
			field: &schema.Field{
				DBName:   uuid.New().String(),
				DataType: schema.Bool,
			},
			nullable:  true,
			typesType: types.Optional(types.TypeBool),
		},
		{
			field: &schema.Field{
				DBName:   uuid.New().String(),
				DataType: schema.Bool,
			},
			nullable:  true,
			typesType: types.TypeBool,
			options: []TypeByYdbTypeOption{
				func(columnType *migrator.ColumnType) {
					columnType.NullableValue = sql.NullBool{
						Bool:  true,
						Valid: true,
					}
				},
			},
		},
		{
			field: &schema.Field{
				DBName:   uuid.New().String(),
				DataType: schema.Int,
				Size:     32,
			},
			typesType: types.TypeInt32,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			columnType, typesType, err := TypeByYdbType(
				tt.field,
				tt.typesType,
				tt.options...,
			)
			require.NoError(t, err)

			if isOptional, innerType := types.IsOptional(tt.typesType); isOptional {
				tt.typesType = innerType
			}

			require.Equal(t, tt.typesType, typesType)

			require.Equal(t, tt.field.DBName, columnType.Name())
			require.Equal(t, tt.typesType.Yql(), columnType.DatabaseTypeName())

			primaryKey, ok := columnType.PrimaryKey()
			require.True(t, ok, "primary key not defined")
			require.Equal(t, tt.field.PrimaryKey, primaryKey)

			nullable, ok := columnType.Nullable()
			require.True(t, ok, "nullable not defined")
			require.Equal(t, tt.nullable, nullable)

			length, ok := columnType.Length()
			require.True(t, ok, "length not defined")
			require.Equal(t, int64(tt.field.Size), length)
		})
	}
}

func TestType(t *testing.T) {
	tests := []struct {
		field     *schema.Field
		typesType types.Type
		isError   bool
	}{
		{
			field: &schema.Field{
				DataType: schema.Bool,
			},
			typesType: types.TypeBool,
		},
		{
			field: &schema.Field{
				DataType: schema.Int,
				Size:     8,
			},
			typesType: types.TypeInt8,
		},
		{
			field: &schema.Field{
				DataType: schema.Int,
				Size:     16,
			},
			typesType: types.TypeInt16,
		},
		{
			field: &schema.Field{
				DataType: schema.Int,
				Size:     32,
			},
			typesType: types.TypeInt32,
		},
		{
			field: &schema.Field{
				DataType: schema.Int,
				Size:     64,
			},
			typesType: types.TypeInt64,
		},
		{
			field: &schema.Field{
				DataType: schema.Uint,
				Size:     8,
			},
			typesType: types.TypeUint8,
		},
		{
			field: &schema.Field{
				DataType: schema.Uint,
				Size:     16,
			},
			typesType: types.TypeUint16,
		},
		{
			field: &schema.Field{
				DataType: schema.Uint,
				Size:     32,
			},
			typesType: types.TypeUint32,
		},
		{
			field: &schema.Field{
				DataType: schema.Uint,
				Size:     64,
			},
			typesType: types.TypeUint64,
		},
		{
			field: &schema.Field{
				DataType: schema.Float,
				Size:     32,
			},
			typesType: types.TypeFloat,
		},
		{
			field: &schema.Field{
				DataType: schema.Float,
				Size:     64,
			},
			typesType: types.TypeDouble,
		},
		{
			field: &schema.Field{
				DataType: schema.String,
			},
			typesType: types.TypeText,
		},
		{
			field: &schema.Field{
				DataType: schema.Bytes,
			},
			typesType: types.TypeBytes,
		},
		{
			field: &schema.Field{
				DataType: schema.Time,
			},
			typesType: types.TypeTimestamp,
		},
		{
			field:   &schema.Field{},
			isError: true,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			columnType, typesType, err := Type(tt.field)
			if tt.isError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tt.typesType, typesType)

			require.Equal(t, tt.typesType.Yql(), columnType.DatabaseTypeName())

			length, ok := columnType.Length()
			require.True(t, ok, "length not defined")
			require.Equal(t, int64(tt.field.Size), length)
		})
	}
}
