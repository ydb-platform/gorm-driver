package ydb_test

import (
	"context"
	"fmt"

	ydb "github.com/ydb-platform/gorm-driver"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"gorm.io/gorm"
)

func Example_query() {
	type Product struct {
		ID    uint `gorm:"primarykey;not null;autoIncrement:false"`
		Code  string
		Price uint `gorm:"index"`
	}

	db, err := gorm.Open(
		ydb.Open("grpc://localhost:2136/local",
			ydb.With(environ.WithEnvironCredentials()),
		),
	)
	if err != nil {
		panic(err)
	}

	db = db.Debug()

	// Migrate the schema
	err = db.AutoMigrate(&Product{})
	if err != nil {
		panic(err)
	}

	// Create
	err = db.Create(&Product{ID: 1, Code: "D42", Price: 100}).Error
	if err != nil {
		panic(err)
	}

	// Scan query
	var products []Product
	err = db.
		WithContext(ydb.WithQueryMode(context.Background(), ydb.ScanQueryMode)).
		Model(&Product{}).
		Scan(&products).
		Error
	if err != nil {
		panic(err)
	}

	// Read
	var product Product
	err = db.First(&product, 1).Error // find product with integer primary key
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", product)

	err = db.First(&product, "code = ?", "D42").Error // find product with code D42
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n", product)

	// Update - update product's price to 200
	err = db.Model(&product).Update("Price", 200).Error
	if err != nil {
		panic(err)
	}

	// Update - update multiple fields
	err = db.Model(&product).Updates(Product{Price: 200, Code: "F42"}).Error // non-zero fields
	if err != nil {
		panic(err)
	}

	err = db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"}).Error
	if err != nil {
		panic(err)
	}

	// Delete - delete product
	err = db.Delete(&product, 1).Error
	if err != nil {
		panic(err)
	}

	// Drop table
	err = db.Migrator().DropTable(&Product{})
	if err != nil {
		panic(err)
	}
}
