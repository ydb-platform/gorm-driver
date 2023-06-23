# GORM YDB Driver

YDB support for GORM

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/ydb-platform/gorm-driver.svg)](https://github.com/ydb-platform/gorm-driver/releases)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/gorm-driver)](https://pkg.go.dev/github.com/ydb-platform/gorm-driver)
![tests](https://github.com/ydb-platform/gorm-driver/workflows/tests/badge.svg?branch=main)
![lint](https://github.com/ydb-platform/gorm-driver/workflows/lint/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/gorm-driver)](https://goreportcard.com/report/github.com/ydb-platform/gorm-driver)
[![codecov](https://codecov.io/gh/ydb-platform/gorm-driver/branch/main/graph/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/gorm-driver)
![Code lines](https://sloc.xyz/github/ydb-platform/gorm-driver/?category=code)
[![View examples](https://img.shields.io/badge/learn-examples-brightgreen.svg)](https://github.com/ydb-platform/ydb-go-sdk/tree/master/examples/basic/gorm)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ydb_en)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)

## Quick Start

You can simply test your connection to your database with the following:

```go
package main

import (
	ydb "github.com/ydb-platform/gorm-driver"
	"gorm.io/gorm"
)

type User struct {
	Name string `gorm:"primarykey"`
	Age  int
}

func main() {
	db, err := gorm.Open(ydb.Open("grpc://localhost:2136/local"))
	if err != nil {
		panic("failed to connect database")
	}

	// Auto Migrate
	db.AutoMigrate(&User{})

	// Insert
	db.Create(&User{Name: "Angeliz", Age: 18})

	// Select
	db.Find(&User{}, "name = ?", "Angeliz")

	// Batch Insert
	user1 := User{Name: "Charles", Age: 12}
	user2 := User{Name: "Feynman", Age: 13}
	user3 := User{Name: "Michael", Age: 14}
	users := []User{user1, user2, user3}
	db.Create(&users)

	// ...
}
```