package dialect

import "gorm.io/gorm"

func checkAndAddError(stmt *gorm.Statement, err error) {
	if err != nil {
		_ = stmt.AddError(err)
	}
}
