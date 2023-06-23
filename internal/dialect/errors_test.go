package dialect

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func Test_checkAndAddError(t *testing.T) {
	t.Run("", func(t *testing.T) {
		stmt := gorm.Statement{
			DB: &gorm.DB{
				Config: &gorm.Config{},
			},
		}

		checkAndAddError(&stmt, nil)

		require.Nil(t, stmt.Error)
	})

	t.Run("", func(t *testing.T) {
		stmt := gorm.Statement{
			DB: &gorm.DB{
				Config: &gorm.Config{},
			},
		}
		err := errors.New("some error")

		checkAndAddError(&stmt, err)

		require.ErrorIs(t, stmt.Error, err)
	})

	t.Run("", func(t *testing.T) {
		stmt := gorm.Statement{
			DB: &gorm.DB{
				Config: &gorm.Config{},
			},
		}
		err := errors.New("some error")
		anotherErr := fmt.Errorf("another error: %w", err)

		checkAndAddError(&stmt, anotherErr)

		require.ErrorIs(t, stmt.Error, anotherErr)
		require.ErrorIs(t, stmt.Error, err)
	})
}
