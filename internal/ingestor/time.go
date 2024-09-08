package ingestor

import (
	"strings"
	"time"
)

type interval string
type intervalOptions []interval

func (option interval) Match(input string) bool {
	return strings.ToUpper(input) == string(option)
}

func (options intervalOptions) Includes(input string) bool {
	for _, i := range options {
		if i.Match(input) {
			return true
		}
	}
	return false
}

const (
	ONE_MINUTE      = time.Minute
	FIVE_MINUTES    = 5 * time.Minute
	TEN_MINUTES     = 10 * time.Minute
	FIFTEEN_MINUTES = 15 * time.Minute
	THIRTY_MINUTES  = 30 * time.Minute
	ONE_HOUR        = time.Hour
	SIX_HOURS       = 6 * time.Hour
	TWELVE_HOURS    = 12 * time.Hour
	ONE_DAY         = 24 * time.Hour
	ONE_WEEK        = 7 * ONE_DAY
	ONE_MONTH       = 30 * ONE_DAY
)

var ValidTimeIntervals intervalOptions = intervalOptions{
	"ONE_MINUTE",
	"FIVE_MINUTES",
	"TEN_MINUTES",
	"FIFTEEN_MINUTES",
	"THIRTY_MINUTES",
	"ONE_HOUR",
	"SIX_HOURS",
	"TWELVE_HOURS",
	"ONE_DAY",
	"ONE_WEEK",
	"ONE_MONTH",
}

var TimeIntervalToDurationMapping map[string]time.Duration = map[string]time.Duration{
	"ONE_MINUTE":      ONE_MINUTE,
	"FIVE_MINUTES":    FIVE_MINUTES,
	"TEN_MINUTES":     TEN_MINUTES,
	"FIFTEEN_MINUTES": FIFTEEN_MINUTES,
	"THIRTY_MINUTES":  THIRTY_MINUTES,
	"ONE_HOUR":        ONE_HOUR,
	"SIX_HOURS":       SIX_HOURS,
	"TWELVE_HOURS":    TWELVE_HOURS,
	"ONE_DAY":         ONE_DAY,
	"ONE_WEEK":        ONE_WEEK,
	"ONE_MONTH":       ONE_MONTH,
}

type TimeRange interface {
	Start() time.Time
	End() time.Time
}

type DurationRange struct {
	duration time.Duration
}

func (dr DurationRange) Start() time.Time {
	return time.Now().Add(-dr.duration)
}

func (dr DurationRange) End() time.Time {
	return time.Now()
}

func NewDurationRange(d time.Duration) DurationRange {
	return DurationRange{duration: d}
}
