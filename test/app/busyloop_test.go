package main

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestBusyLoopDuration(t *testing.T) {
	// test busyLoopDuration with different levels of concurrency
	// for each concurrency level, run busyLoopDuration multiple times and calculate mean and std to see if wall clock time is of executions are close to given duration (calculate mean and std)

	concurrencies := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}
	duration := 100
	numRuns := 10

	for _, concurrency := range concurrencies {
		var totalDuration time.Duration
		var durations []time.Duration

		for i := 0; i < numRuns; i++ {
			start := time.Now()
			var wg sync.WaitGroup
			wg.Add(concurrency)
			for j := 0; j < concurrency; j++ {
				go func() {
					defer wg.Done()
					busyLoopDuration(duration)
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			durations = append(durations, elapsed)
			totalDuration += elapsed
		}

		mean := totalDuration / time.Duration(numRuns)
		var variance float64
		for _, d := range durations {
			diff := float64(d - mean)
			variance += diff * diff
		}
		stdDev := time.Duration(math.Sqrt(variance / float64(numRuns)))

		t.Logf("Concurrency: %d, Mean: %v, StdDev: %v", concurrency, mean, stdDev)
	}

}
