package dialect

import (
	"bytes"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

func TestWith(t *testing.T) {
	d := &Dialector{}
	var opt ydb.Option

	With(opt)(d)

	require.Contains(t, d.opts, opt)
}

func TestWithTablePathPrefix(t *testing.T) {
	d := &Dialector{}
	tablePathPrefix := "gormPrefix"

	WithTablePathPrefix(tablePathPrefix)(d)

	require.Equal(t, tablePathPrefix, d.tablePathPrefix)
}

func TestWithMaxOpenConns(t *testing.T) {
	d := &Dialector{}
	maxOpenConns := 100

	WithMaxOpenConns(maxOpenConns)(d)

	require.Equal(t, maxOpenConns, d.maxOpenConns)
}

func TestWithMaxIdleConns(t *testing.T) {
	d := &Dialector{}
	maxIdleConns := 200

	WithMaxIdleConns(maxIdleConns)(d)

	require.Equal(t, maxIdleConns, d.maxIdleConns)
}

func TestWithConnMaxIdleTime(t *testing.T) {
	d := &Dialector{}
	connMaxIdleTime := time.Minute

	WithConnMaxIdleTime(connMaxIdleTime)(d)

	require.Equal(t, connMaxIdleTime, d.connMaxIdleTime)
}

func TestNew(t *testing.T) {
	dsn := "dataSourceName"
	tablePathPrefix := "tablePathPrefix"

	d := New(dsn, WithTablePathPrefix(tablePathPrefix))
	require.Equal(t, dsn, d.DSN)
	require.Equal(t, tablePathPrefix, d.tablePathPrefix)
}

func TestDialector_Name(t *testing.T) {
	d := &Dialector{}

	require.Equal(t, "ydb", d.Name())
}

func TestDialector_Initialize(t *testing.T) {
	t.Run("wrong DSN", func(t *testing.T) {
		d := Dialector{
			DSN: "",
		}
		db := gorm.DB{
			Config: &gorm.Config{
				ConnPool: &sql.DB{},
			},
		}

		err := d.Initialize(&db)
		require.Error(t, err)
	})
}

func TestDialector_Migrator(t *testing.T) {
	db := &gorm.DB{}
	d := Dialector{tablePathPrefix: "tablePathPrefix"}

	exp := migrator.Migrator{
		Config: migrator.Config{
			DB:        db,
			Dialector: d,
		},
	}

	m, ok := d.Migrator(db).(Migrator)
	require.True(t, ok)
	require.Equal(t, exp, m.Migrator)
}

func TestDialector_DataTypeOf(t *testing.T) {
	tests := []struct {
		field    *schema.Field
		dataType string
		isPanics bool
	}{
		{
			field: &schema.Field{
				DataType: schema.Bool,
			},
			dataType: "Bool",
		},
		{
			field:    &schema.Field{},
			isPanics: true,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			d := Dialector{}
			if tt.isPanics {
				require.Panics(t, func() {
					d.DataTypeOf(tt.field)
				})

				return
			}
			require.Equal(t, tt.dataType, d.DataTypeOf(tt.field))
		})
	}
}

func TestDialector_DefaultValueOf(t *testing.T) {
	d := Dialector{}
	require.Panics(t, func() {
		d.DefaultValueOf(nil)
	})
}

func TestDialector_QuoteTo(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{
			input:  "",
			output: "",
		},
		{
			input:  "`",
			output: "``",
		},
		{
			input:  "``",
			output: "``",
		},
		{
			input:  "`foo",
			output: "`foo`",
		},
		{
			input:  "foo`",
			output: "`foo```",
		},
		{
			input:  "`foo`",
			output: "`foo`",
		},
		{
			input:  "foo.bar",
			output: "`foo`.`bar`",
		},
		{
			input:  "`foo.bar",
			output: "`foo.bar`",
		},
		{
			input:  "`foo`.bar",
			output: "`foo`.`bar`",
		},
		{
			input:  "foo.bar.baz",
			output: "`foo`.`bar`.`baz`",
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			d := Dialector{}
			writer := &bytes.Buffer{}

			d.QuoteTo(writer, tt.input)

			require.Equal(t, tt.output, writer.String())
		})
	}
}

func TestDialector_Explain(t *testing.T) {
	tests := []struct {
		sql      string
		vars     []interface{}
		expected string
	}{
		{
			sql: "INSERT INTO table (id, payload) VALUES (?, ?)",
			vars: []interface{}{
				"entryID",
				"sometext",
			},
			expected: "INSERT INTO table (id, payload) VALUES ('entryID', 'sometext')",
		},
		{
			sql: "INSERT INTO table (id, payload) VALUES (?, ?)",
			vars: []interface{}{
				"entryID",
				123,
			},
			expected: "INSERT INTO table (id, payload) VALUES ('entryID', 123)",
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			d := Dialector{}

			require.Equal(t, tt.expected, d.Explain(tt.sql, tt.vars...))
		})
	}
}

func TestDialector_BindVarTo(t *testing.T) {
	tests := []struct {
		vars     []interface{}
		expected string
	}{
		{
			vars:     []interface{}{},
			expected: "$0",
		},
		{
			vars:     []interface{}{1},
			expected: "$1",
		},
		{
			vars:     []interface{}{1, "asd"},
			expected: "$2",
		},
		{
			vars:     []interface{}{1, "asd", time.Second},
			expected: "$3",
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			stmt := &gorm.Statement{
				DB: &gorm.DB{
					Config: &gorm.Config{},
				},
				Vars: tt.vars,
			}

			d := Dialector{}
			writer := &bytes.Buffer{}

			d.BindVarTo(writer, stmt, nil)

			require.NoError(t, stmt.Error)
			require.Equal(t, tt.expected, writer.String())
		})
	}
}
