package integration

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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

	for i := 0; i < 5; i++ {
		t.Run("", func(t *testing.T) {
			db, err := gorm.Open(
				ydb.Open(dsn,
					ydb.WithTablePathPrefix(t.Name()),
				),
			)
			require.NoError(t, err)
			require.NotNil(t, db)

			db = db.Debug()

			err = db.AutoMigrate(&Migrations{})
			require.NoError(t, err)
		})
	}
}

func TestMigrateColumn(t *testing.T) {
	dsn, has := os.LookupEnv("YDB_CONNECTION_STRING")
	if !has {
		t.Skip("skip test '" + t.Name() + "' without env 'YDB_CONNECTION_STRING'")
	}

	db, err := gorm.Open(
		ydb.Open(dsn,
			ydb.WithTablePathPrefix(t.Name()),
		),
	)
	require.NoError(t, err)
	require.NotNil(t, db)

	db = db.Debug()

	type dataType struct {
		Type     string
		Nullable bool
	}

	checkDataTypes := func(t *testing.T, expected map[string]dataType, actual []gorm.ColumnType) {
		require.Equal(t, len(expected), len(actual), "rows count didn't match")
		for _, actualType := range actual {
			expectedType, ok := expected[actualType.Name()]
			require.True(t, ok)

			require.Equal(t, expectedType.Type, actualType.DatabaseTypeName(), "database type didn't match")

			nullable, ok := actualType.Nullable()
			require.True(t, ok, "nullable not supported")
			require.Equal(t, expectedType.Nullable, nullable, "nullable didn't match")
		}
	}

	t.Run("create table", func(t *testing.T) {
		type migrationsTable struct {
			ID      string `gorm:"primarykey;not null"`
			Payload string
		}

		err = db.AutoMigrate(new(migrationsTable))
		require.NoError(t, err)

		hasTable := db.Migrator().HasTable(new(migrationsTable))
		require.True(t, hasTable)

		var columnTypes []gorm.ColumnType
		columnTypes, err = db.Migrator().ColumnTypes(new(migrationsTable))
		require.NoError(t, err)

		checkDataTypes(t, map[string]dataType{
			"id": {
				Type:     types.TypeText.String(),
				Nullable: false,
			},
			"payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
		}, columnTypes)
	})

	t.Run("add column with AutoMigrate", func(t *testing.T) {
		type migrationsTable struct {
			ID             string `gorm:"primarykey;not null"`
			Payload        string
			AnotherPayload string
		}

		err = db.AutoMigrate(new(migrationsTable))
		require.NoError(t, err)

		var columnTypes []gorm.ColumnType
		columnTypes, err = db.Migrator().ColumnTypes(new(migrationsTable))
		require.NoError(t, err)

		checkDataTypes(t, map[string]dataType{
			"id": {
				Type:     types.TypeText.String(),
				Nullable: false,
			},
			"payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
			"another_payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
		}, columnTypes)
	})

	t.Run("add column with Migrator.AddColumn()", func(t *testing.T) {
		type migrationsTable struct {
			ID             string `gorm:"primarykey;not null"`
			Payload        string
			AnotherPayload string
			OneMorePayload string
		}

		err = db.Migrator().AddColumn(new(migrationsTable), "OneMorePayload")
		require.NoError(t, err)

		var columnTypes []gorm.ColumnType
		columnTypes, err = db.Migrator().ColumnTypes(new(migrationsTable))
		require.NoError(t, err)

		checkDataTypes(t, map[string]dataType{
			"id": {
				Type:     types.TypeText.String(),
				Nullable: false,
			},
			"payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
			"another_payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
			"one_more_payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
		}, columnTypes)
	})

	t.Run("drop column with Migrator.DropColumn()", func(t *testing.T) {
		type migrationsTable struct {
			ID             string `gorm:"primarykey;not null"`
			Payload        string
			AnotherPayload string
			OneMorePayload string
		}

		err = db.Migrator().DropColumn(new(migrationsTable), "AnotherPayload")
		require.NoError(t, err)

		var columnTypes []gorm.ColumnType
		columnTypes, err = db.Migrator().ColumnTypes(new(migrationsTable))
		require.NoError(t, err)

		checkDataTypes(t, map[string]dataType{
			"id": {
				Type:     types.TypeText.String(),
				Nullable: false,
			},
			"payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
			"one_more_payload": {
				Type:     types.TypeText.String(),
				Nullable: true,
			},
		}, columnTypes)
	})

	t.Run("error on AlterColumn with AutoMigrate()", func(t *testing.T) {
		type migrationsTable struct {
			ID             string `gorm:"primarykey;not null"`
			Payload        string
			OneMorePayload uint64
		}

		err = db.AutoMigrate(new(migrationsTable))
		require.Error(t, err)
	})

	t.Run("drop table", func(t *testing.T) {
		type migrationsTable struct {
			ID             string `gorm:"primarykey;not null"`
			Payload        string
			OneMorePayload string
		}

		err = db.Migrator().DropTable(new(migrationsTable))
		require.NoError(t, err)

		hasTable := db.Migrator().HasTable(new(migrationsTable))
		require.False(t, hasTable)
	})
}
