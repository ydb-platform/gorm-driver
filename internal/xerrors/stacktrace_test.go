package xerrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStacktraceError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStacktrace(fmt.Errorf("fmt.Errorf")),
			//nolint:lll
			text: "fmt.Errorf at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStacktraceError(stacktrace_test.go:17)`",
		},
		{
			error: WithStacktrace(fmt.Errorf("fmt.Errorf %s", "Printf")),
			//nolint:lll
			text: "fmt.Errorf Printf at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStacktraceError(stacktrace_test.go:22)`",
		},
		{
			error: WithStacktrace(
				WithStacktrace(errors.New("errors.New")),
			),
			//nolint:lll
			text: "errors.New at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStacktraceError(stacktrace_test.go:28)` at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStacktraceError(stacktrace_test.go:27)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			require.Equal(t, test.text, test.error.Error())
		})
	}
}

func TestWithStacktrace(t *testing.T) {
	err := WithStacktrace(nil)
	require.Nil(t, err)
}

func Test_fileName(t *testing.T) {
	tests := []struct {
		path     string
		fileName string
	}{
		{path: "foo/bar/baz.log", fileName: "baz.log"},
		{path: "foo/bar.log", fileName: "bar.log"},
		{path: "foo.log", fileName: "foo.log"},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			name := fileName(tt.path)
			require.Equal(t, tt.fileName, name)
		})
	}
}

func Test_stackError(t *testing.T) {
	s := stackError{
		stackRecord: "foo/bar/baz.log",
		err:         errors.New("some error"),
	}

	msg := s.Error()
	require.Equal(t, "some error at `foo/bar/baz.log`", msg)

	err := s.Unwrap()
	require.Equal(t, s.err, err)
}
