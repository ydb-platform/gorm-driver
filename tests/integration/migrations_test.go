package integration

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbDriver "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
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

	pathPrefix := t.Name()

	for i := 0; i < 5; i++ {
		t.Run("", func(t *testing.T) {
			db, err := gorm.Open(
				ydb.Open(dsn,
					ydb.WithTablePathPrefix(pathPrefix),
					ydb.With(environ.WithEnvironCredentials()),
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

//nolint:funlen
func TestMigrateColumn(t *testing.T) {
	dsn, has := os.LookupEnv("YDB_CONNECTION_STRING")
	if !has {
		t.Skip("skip test '" + t.Name() + "' without env 'YDB_CONNECTION_STRING'")
	}

	db, err := gorm.Open(
		ydb.Open(dsn,
			ydb.WithTablePathPrefix(t.Name()),
			ydb.With(environ.WithEnvironCredentials()),
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

func TestCreateTableWithOptions(t *testing.T) {
	partitionSize := uint64(100)
	minPartitionsCount := uint64(1)
	maxPartitionsCount := uint64(10)

	tableOptions := fmt.Sprintf(`WITH (
		AUTO_PARTITIONING_BY_SIZE = ENABLED,
		AUTO_PARTITIONING_BY_LOAD = ENABLED,
		AUTO_PARTITIONING_PARTITION_SIZE_MB = %d,
		AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d,
		AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d
	)`, partitionSize, minPartitionsCount, maxPartitionsCount)

	type Product struct {
		ID    uint `gorm:"primarykey;not null;autoIncrement:false"`
		Code  string
		Price uint
	}

	dsn, has := os.LookupEnv("YDB_CONNECTION_STRING")
	if !has {
		t.Skip("skip test '" + t.Name() + "' without env 'YDB_CONNECTION_STRING'")
	}

	db, err := gorm.Open(
		ydb.Open(dsn,
			ydb.WithTablePathPrefix(t.Name()),
			ydb.With(environ.WithEnvironCredentials()),
		),
	)
	require.NoError(t, err)
	require.NotNil(t, db)

	db = db.Debug()

	err = db.Set("gorm:table_options", tableOptions).AutoMigrate(&Product{})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	driver, err := ydbDriver.Unwrap(sqlDB)
	require.NoError(t, err)

	var desc options.Description
	err = driver.Table().Do(context.Background(), func(ctx context.Context, s table.Session) (err error) {
		desc, err = s.DescribeTable(ctx, path.Join(driver.Name(), t.Name(), "products"))

		return err
	}, table.WithIdempotent())
	require.NoError(t, err)

	require.Equal(t, options.FeatureEnabled, desc.PartitioningSettings.PartitioningBySize)
	require.Equal(t, options.FeatureEnabled, desc.PartitioningSettings.PartitioningByLoad)
	require.Equal(t, partitionSize, desc.PartitioningSettings.PartitionSizeMb)
	require.Equal(t, minPartitionsCount, desc.PartitioningSettings.MinPartitionsCount)
	require.Equal(t, maxPartitionsCount, desc.PartitioningSettings.MaxPartitionsCount)
}
