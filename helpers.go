package rabbitmq

import (
	"strconv"
	"time"
)

func durationToMillisecondsString(duration time.Duration) string {
	return strconv.FormatInt(duration.Milliseconds(), 10)
}
