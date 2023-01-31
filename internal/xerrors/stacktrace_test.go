package xerrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStackTraceError(t *testing.T) {
	for _, test := range []struct {
		error error
		text  string
	}{
		{
			error: WithStackTrace(fmt.Errorf("fmt.Errorf")),
			//nolint:lll
			text: "fmt.Errorf at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStackTraceError(stacktrace_test.go:17)`",
		},
		{
			error: WithStackTrace(fmt.Errorf("fmt.Errorf %s", "Printf")),
			//nolint:lll
			text: "fmt.Errorf Printf at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStackTraceError(stacktrace_test.go:22)`",
		},
		{
			error: WithStackTrace(
				WithStackTrace(errors.New("errors.New")),
			),
			//nolint:lll
			text: "errors.New at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStackTraceError(stacktrace_test.go:28)` at `github.com/ydb-platform/gorm-driver/internal/xerrors.TestStackTraceError(stacktrace_test.go:27)`",
		},
	} {
		t.Run(test.text, func(t *testing.T) {
			require.Equal(t, test.text, test.error.Error())
		})
	}
}
