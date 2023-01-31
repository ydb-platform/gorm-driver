package ydb

import (
	"github.com/ydb-platform/gorm-driver/internal/dialect"
	"gorm.io/gorm"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Option = dialect.Option

func With(opts ...ydb.Option) Option {
	return dialect.With(opts...)
}

func WithTablePathPrefix(tablePathPrefix string) Option {
	return dialect.WithTablePathPrefix(tablePathPrefix)
}

func Open(dsn string, opts ...Option) gorm.Dialector {
	return dialect.New(dsn, opts...)
}
