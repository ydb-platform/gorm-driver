package ydb_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	ydb "github.com/ydb-platform/gorm-driver"
)

type Product struct {
	gorm.Model
	Code  string
	Price uint
}

func TestDriver(t *testing.T) {
	dsn, has := os.LookupEnv("YDB_CONNECTION_STRING")
	if !has {
		t.Skip("skip test '" + t.Name() + "' without env 'YDB_CONNECTION_STRING'")
	}

	db, err := gorm.Open(ydb.Open(dsn))
	require.NoError(t, err)
	require.NotNil(t, db)

	// Migrate the schema
	err = db.AutoMigrate(&Product{})
	require.NoError(t, err)

	// Create
	err = db.Create(&Product{Code: "D42", Price: 100}).Error
	require.NoError(t, err)

	// Read
	var product Product
	err = db.First(&product, 1).Error // find product with integer primary key
	require.NoError(t, err)

	err = db.First(&product, "code = ?", "D42").Error // find product with code D42
	require.NoError(t, err)

	// Update - update product's price to 200
	err = db.Model(&product).Update("Price", 200).Error
	require.NoError(t, err)

	// Update - update multiple fields
	err = db.Model(&product).Updates(Product{Price: 200, Code: "F42"}).Error // non-zero fields
	require.NoError(t, err)

	err = db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"}).Error
	require.NoError(t, err)

	// Delete - delete product
	err = db.Delete(&product, 1).Error
	require.NoError(t, err)
}
