package column

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var TypeAliasMap = map[string][]string{
	"int2":     {"smallint"},
	"int4":     {"integer"},
	"int8":     {"bigint"},
	"smallint": {"int2"},
	"integer":  {"int4"},
	"bigint":   {"int8"},
	"decimal":  {"numeric"},
	"numeric":  {"decimal"},
}

type TypeByYdbTypeOption func(columnType *migrator.ColumnType)

func WithDefault(t types.Type) TypeByYdbTypeOption {
	return func(columnType *migrator.ColumnType) {
		columnType.DefaultValueValue = sql.NullString{
			String: types.ZeroValue(t).Yql(),
			Valid:  true,
		}
	}
}

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
		return TypeByYdbType(f, types.TypeDatetime)
	default:
		return nil, nil, fmt.Errorf("unsupported data type '%s'", f.DataType)
	}
}

func Value(value interface{}) types.Value {
	switch v := value.(type) {
	case bool:
		return types.BoolValue(v)
	case int8:
		return types.Int8Value(v)
	case uint8:
		return types.Uint8Value(v)
	case int16:
		return types.Int16Value(v)
	case uint16:
		return types.Uint16Value(v)
	case int32:
		return types.Int32Value(v)
	case uint32:
		return types.Uint32Value(v)
	case int64:
		return types.Int64Value(v)
	case uint64:
		return types.Uint64Value(v)
	case float32:
		return types.FloatValue(v)
	case float64:
		return types.DoubleValue(v)
	case string:
		return types.TextValue(v)
	case []byte:
		return types.BytesValue(v)
	case time.Time:
		return types.DatetimeValueFromTime(v)
	case time.Duration:
		return types.IntervalValueFromDuration(v)
	case gorm.DeletedAt:
		return types.DatetimeValueFromTime(v.Time)
	default:
		panic(fmt.Sprintf("unsupported type %+v", v))
	}
}

type MyTryLockMutex struct {
	sema chan struct{}
}

func New() *MyTryLockMutex {
	return &MyTryLockMutex{
		sema: make(chan struct{}, 1),
	}
}

func (m *MyTryLockMutex) Lock() {
	m.sema <- struct{}{}
}

func (m *MyTryLockMutex) Unlock() {
	<-m.sema
}

func (m *MyTryLockMutex) TryLock(ctx context.Context) bool {
	select {
	case m.sema <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}
