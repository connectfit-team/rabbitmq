package rabbitmq

import (
	"strconv"
	"strings"
	"time"
)

func joinStringsWithDots(strs ...string) string {
	return strings.Join(strs, ".")
}

func durationToMillisecondsString(duration time.Duration) string {
	return strconv.FormatInt(duration.Milliseconds(), 10)
}
