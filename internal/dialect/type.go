package dialect

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

// toColumnTypeOption is option type for toColumnType.
type toColumnTypeOption func(columnType *migrator.ColumnType) error

// toColumnType generate gorm.ColumnType from schema.Field and ydb Type.
func toColumnType(f *schema.Field, t types.Type, opts ...toColumnTypeOption) (gorm.ColumnType, error) {
	nullable := false
	isOptional, innerType := types.IsOptional(t)
	for isOptional {
		nullable = true
		t = innerType
		isOptional, innerType = types.IsOptional(t)
	}

	columnType := migrator.ColumnType{
		NameValue: sql.NullString{
			String: f.DBName,
			Valid:  true,
		},
		DataTypeValue: sql.NullString{
			String: t.Yql(),
			Valid:  true,
		},
		PrimaryKeyValue: sql.NullBool{
			Bool:  f.PrimaryKey,
			Valid: true,
		},
		DecimalSizeValue: sql.NullInt64{
			Int64: 0,
			Valid: true,
		},
		NullableValue: sql.NullBool{
			Bool:  nullable,
			Valid: true,
		},
		LengthValue: sql.NullInt64{
			Int64: int64(f.Size),
			Valid: true,
		},
	}
	for _, opt := range opts {
		err := opt(&columnType)
		if err != nil {
			return nil, err
		}
	}

	return columnType, nil
}

// parseField parse schema.Field and generate gorm.ColumnType with ydb Type.
func parseField(f *schema.Field) (gorm.ColumnType, types.Type, error) {
	wrapType := func(t types.Type) (gorm.ColumnType, types.Type, error) {
		ct, err := toColumnType(f, t)

		return ct, t, err
	}
	if tp, ok := f.TagSettings["TYPE"]; ok {
		tp = strings.TrimSpace(tp)
		switch strings.ToLower(tp) {
		case "json":
			if f.Serializer == nil {
				f.Serializer = DefaultYdbJSONSerializer{}
			}
			return wrapType(types.TypeJSON)
		case "jsondocument":
			if f.Serializer == nil {
				f.Serializer = DefaultYdbJSONSerializer{}
			}
			return wrapType(types.TypeJSONDocument)
		case "yson":
			if f.Serializer == nil {
				f.Serializer = DefaultYdbJSONSerializer{}
			}
			return wrapType(types.TypeYSON)
		}
	}

	if v, ok := f.TagSettings["SERIALIZER"]; ok && strings.EqualFold(v, "json") {
		f.Serializer = schema.JSONSerializer{}
		return wrapType(types.TypeJSONDocument)
	}

	switch f.DataType {
	case schema.Bool:
		return wrapType(types.TypeBool)
	case schema.Int:
		switch {
		case f.Size <= 8:
			return wrapType(types.TypeInt8)
		case f.Size <= 16:
			return wrapType(types.TypeInt16)
		case f.Size <= 32:
			return wrapType(types.TypeInt32)
		default:
			return wrapType(types.TypeInt64)
		}
	case schema.Uint:
		switch {
		case f.Size <= 8:
			return wrapType(types.TypeUint8)
		case f.Size <= 16:
			return wrapType(types.TypeUint16)
		case f.Size <= 32:
			return wrapType(types.TypeUint32)
		default:
			return wrapType(types.TypeUint64)
		}
	case schema.Float:
		switch {
		case f.Size <= 32:
			return wrapType(types.TypeFloat)
		default:
			return wrapType(types.TypeDouble)
		}
	case schema.String:
		return wrapType(types.TypeText)
	case schema.Bytes:
		return wrapType(types.TypeBytes)
	case schema.Time:
		return wrapType(types.TypeTimestamp)
	default:
		return nil, nil, xerrors.WithStacktrace(fmt.Errorf("unsupported data type '%s'", f.DataType))
	}
}
