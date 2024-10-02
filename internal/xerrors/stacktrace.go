package xerrors

import (
	"runtime"
	"strconv"
	"strings"
)

// WithStacktrace is a wrapper over original err with file:line identification
func WithStacktrace(err error) error {
	if err == nil {
		return nil
	}

	return &stackError{
		stackRecord: stackRecord(1),
		err:         err,
	}
}

func stackRecord(depth int) string {
	function, file, line, _ := runtime.Caller(depth + 1)
	name := runtime.FuncForPC(function).Name()

	return name + "(" + fileName(file) + ":" + strconv.Itoa(line) + ")"
}

func fileName(original string) string {
	i := strings.LastIndex(original, "/")
	if i == -1 {
		return original
	}

	return original[i+1:]
}

type stackError struct {
	stackRecord string
	err         error
}

func (e *stackError) Error() string {
	return e.err.Error() + " at `" + e.stackRecord + "`"
}

func (e *stackError) Unwrap() error {
	return e.err
}
