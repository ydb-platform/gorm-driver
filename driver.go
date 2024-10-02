package ydb

import (
	"context"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"gorm.io/gorm"

	"github.com/ydb-platform/gorm-driver/internal/dialect"
)

type Option = dialect.Option

func With(opts ...ydb.Option) Option {
	return dialect.With(opts...)
}

func WithTablePathPrefix(tablePathPrefix string) Option {
	return dialect.WithTablePathPrefix(tablePathPrefix)
}

func WithMaxOpenConns(n int) Option {
	return dialect.WithMaxOpenConns(n)
}

func WithMaxIdleConns(n int) Option {
	return dialect.WithMaxIdleConns(n)
}

func WithConnMaxIdleTime(d time.Duration) Option {
	return dialect.WithConnMaxIdleTime(d)
}

type QueryMode = ydb.QueryMode

const (
	DataQueryMode      = ydb.DataQueryMode
	ExplainQueryMode   = ydb.ExplainQueryMode
	ScanQueryMode      = ydb.ScanQueryMode
	SchemeQueryMode    = ydb.SchemeQueryMode
	ScriptingQueryMode = ydb.ScriptingQueryMode
)

func WithQueryMode(ctx context.Context, mode QueryMode) context.Context {
	return ydb.WithQueryMode(ctx, mode)
}

func Open(dsn string, opts ...Option) gorm.Dialector {
	return dialect.New(dsn, opts...)
}
