package dialect

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"gorm.io/gorm/schema"

	"github.com/ydb-platform/gorm-driver/internal/xerrors"
)

// DefaultYdbJSONSerializer is implementation of default serializer in YDB format
type DefaultYdbJSONSerializer struct{}

// Scan the same as original JSONSerializer
func (DefaultYdbJSONSerializer) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, dbValue interface{}) error {
	return schema.JSONSerializer{}.Scan(ctx, field, dst, dbValue)
}

// Value marshalize the Go value in JSON and wraps it in YDB value type
func (DefaultYdbJSONSerializer) Value(_ context.Context, field *schema.Field, _ reflect.Value, fieldValue interface{}) (interface{}, error) {
	b, err := json.Marshal(fieldValue)
	if err != nil {
		return nil, err
	}
	tp := strings.ToLower(field.TagSettings["TYPE"])
	switch tp {
	case "json":
		return types.JSONValueFromBytes(b), nil
	case "jsondocument":
		return types.JSONDocumentValueFromBytes(b), nil
	case "yson":
		//return types.YSONValueFromBytes(b), nil
		return nil, xerrors.WithStacktrace(fmt.Errorf("Yson serialization not supported yet"))
	default:
		// По умолчанию — обычный Json
		return nil, xerrors.WithStacktrace(fmt.Errorf("cant resolve serializer for field: '%v'", field))
	}
}
