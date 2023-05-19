package dialect

import (
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

type TypeByYdbTypeOption func(columnType *migrator.ColumnType)

func TypeByYdbType(f *schema.Field, t types.Type, opts ...TypeByYdbTypeOption) (gorm.ColumnType, types.Type, error) {
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
			Bool:  true,
			Valid: true,
		},
		LengthValue: sql.NullInt64{
			Int64: int64(f.Size),
			Valid: true,
		},
	}
	for _, opt := range opts {
		opt(&columnType)
	}
	return columnType, t, nil
}

func Type(f *schema.Field) (gorm.ColumnType, types.Type, error) {
	switch f.DataType {
	case schema.Bool:
		return TypeByYdbType(f, types.TypeBool)
	case schema.Int:
		switch {
		case f.Size <= 8:
			return TypeByYdbType(f, types.TypeInt8)
		case f.Size <= 16:
			return TypeByYdbType(f, types.TypeInt16)
		case f.Size <= 32:
			return TypeByYdbType(f, types.TypeInt32)
		default:
			return TypeByYdbType(f, types.TypeInt64)
		}
	case schema.Uint:
		switch {
		case f.Size <= 8:
			return TypeByYdbType(f, types.TypeUint8)
		case f.Size <= 16:
			return TypeByYdbType(f, types.TypeUint16)
		case f.Size <= 32:
			return TypeByYdbType(f, types.TypeUint32)
		default:
			return TypeByYdbType(f, types.TypeUint64)
		}
	case schema.Float:
		switch {
		case f.Size <= 32:
			return TypeByYdbType(f, types.TypeFloat)
		default:
			return TypeByYdbType(f, types.TypeDouble)
		}
	case schema.String:
		return TypeByYdbType(f, types.TypeText)
	case schema.Bytes:
		return TypeByYdbType(f, types.TypeBytes)
	case schema.Time:
		return TypeByYdbType(f, types.TypeTimestamp)
	default:
		return nil, nil, xerrors.WithStacktrace(fmt.Errorf("unsupported data type '%s'", f.DataType))
	}
}
