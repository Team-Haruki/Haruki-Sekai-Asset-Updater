package utils

import (
	"fmt"
	"time"
)

func getTimeArg() string {
	loc, _ := time.LoadLocation("Asia/Tokyo")
	_time := time.Now().In(loc)
	timeFormat := _time.Format("20060102150405")
	return fmt.Sprintf("?t=%s", timeFormat)
}
