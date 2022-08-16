package timehelper

import "time"

func SetTimeMidnight(t *time.Time) {
	year, month, day := t.Date()
	time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}
