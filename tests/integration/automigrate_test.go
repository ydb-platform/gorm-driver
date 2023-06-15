package integration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	ydb "github.com/ydb-platform/gorm-driver"
)

func TestSequentialAutoMigrate(t *testing.T) {
	type Migrations struct {
		ID      string `gorm:"primarykey;not null"`
		Payload string
	}

	dsn, has := os.LookupEnv("YDB_CONNECTION_STRING")
	if !has {
		t.Skip("skip test '" + t.Name() + "' without env 'YDB_CONNECTION_STRING'")
	}

	t.Run("first", func(t *testing.T) {
		db, err := gorm.Open(ydb.Open(dsn))
		require.NoError(t, err)
		require.NotNil(t, db)

		db = db.Debug()

		err = db.AutoMigrate(&Migrations{})
		require.NoError(t, err)
	})

	t.Run("second", func(t *testing.T) {
		db, err := gorm.Open(ydb.Open(dsn))
		require.NoError(t, err)
		require.NotNil(t, db)

		db = db.Debug()

		err = db.AutoMigrate(&Migrations{})
		require.NoError(t, err)
	})
}
