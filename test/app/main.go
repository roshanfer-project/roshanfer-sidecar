// This is a simple http server

package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		fmt.Fprintf(w,
			"This is a long string returned by the server. "+
				"Lorem ipsum dolor sit amet, consectetur adipiscing elit. "+
				"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "+
				"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.")
	})
	http.ListenAndServe(":2007", nil)
}
