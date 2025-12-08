package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

type Target struct {
	Name string
	URL  string
}

func main() {
	targets := []Target{
		{Name: "app", URL: "http://localhost:6000/debug/pprof/trace?seconds=10"},
		//{Name: "backend1", URL: "http://localhost:6001/debug/pprof/profile?seconds=10"},
	}

	// Create profiles directory
	profileDir := "profiles"
	if err := os.MkdirAll(profileDir, 0755); err != nil {
		fmt.Printf("Error creating profiles directory: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(t Target) {
			defer wg.Done()
			fmt.Printf("Starting profile collection for %s...\n", t.Name)

			resp, err := http.Get(t.URL)
			if err != nil {
				fmt.Printf("Error collecting profile for %s: %v\n", t.Name, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("Error collecting profile for %s: status code %d\n", t.Name, resp.StatusCode)
				return
			}

			filename := filepath.Join(profileDir, fmt.Sprintf("%s.prof", t.Name))
			out, err := os.Create(filename)
			if err != nil {
				fmt.Printf("Error creating file for %s: %v\n", t.Name, err)
				return
			}
			defer out.Close()

			_, err = io.Copy(out, resp.Body)
			if err != nil {
				fmt.Printf("Error writing profile for %s: %v\n", t.Name, err)
				return
			}

			fmt.Printf("Profile collected for %s and saved to %s\n", t.Name, filename)
		}(target)
	}

	wg.Wait()
	fmt.Println("All profiles collected.")
}
