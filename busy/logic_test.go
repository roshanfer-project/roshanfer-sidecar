package busy

import (
	"testing"
	"time"
)

/* func TestBusyLoopDuration(t *testing.T) {

	concurrencies := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 3000}
	duration := 100

	for _, concurrency := range concurrencies {
		var totalDuration time.Duration
		var durations []time.Duration

		var wg sync.WaitGroup
		wg.Add(concurrency)
		channel := make(chan time.Duration, concurrency)
		for j := 0; j < concurrency; j++ {
			go func() {
				defer wg.Done()
				start := time.Now()
				busyLoop(duration)
				channel <- time.Since(start)
			}()
		}
		wg.Wait()
		for i := 0; i < concurrency; i++ {
			elapsed := <-channel
			durations = append(durations, elapsed)
			totalDuration += elapsed
		}

		mean := totalDuration / time.Duration(concurrency)
		var variance float64
		for _, d := range durations {
			diff := float64(d - mean)
			variance += diff * diff
		}
		stdDev := math.Sqrt(variance / float64(concurrency))

		t.Logf("Concurrency: %d, Mean: %v, StdDev: %v", concurrency, mean, stdDev)
	}

} */

// Test BusyLoop to check how much time it takes to execute
func TestBusyLoop(t *testing.T) {
	start := time.Now()
	BusyLoop(140)
	t.Log(time.Since(start).Microseconds())
}
