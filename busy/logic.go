package busy

import "time"

func BusyLoop(repeat int) {
	for range repeat {
		for range 10000 {
		}
	}
}

func busyLoopDuration(duration_us int) int {
	spent_us := 0
	counter := 0
	var now time.Time
	for spent_us < duration_us {
		now = time.Now()
		BusyLoop(1)
		spent_us += int(time.Since(now).Microseconds())
		counter++
	}
	return counter
}
