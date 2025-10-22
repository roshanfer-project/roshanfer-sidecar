// This is a simple http server

package main

import (
	"fmt"
	"net/http"
	"test"
	"test/protobuf"
	"test/utils"
	"time"
)

var testVar string
var client protobuf.Backend1Client
var client2 protobuf.Backend1Client

func main() {
	http.HandleFunc("/app", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		appLogic(w, r)
	})
	http.HandleFunc("/app2", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		app2Logic(w, r)
	})
	http.ListenAndServe(":2007", nil)
}

func init() {
	testVar = utils.GetEnvVar("test", true)
	fmt.Printf("testVar: %s\n", testVar)
	if testVar == "test2" {
		conn := test.GetConnBasic(utils.GetEnvVar("AppEgress", true))
		client = protobuf.NewBackend1Client(conn)
		conn = test.GetConnBasic(utils.GetEnvVar("AppEgress", true))
		client2 = protobuf.NewBackend1Client(conn)
	}
}

func appLogic(w http.ResponseWriter, r *http.Request) {
	switch testVar {
	case "test":
		time.Sleep(10 * time.Millisecond)
		fmt.Fprintf(w, "This is a long string returned by the server. "+
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. "+
			"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "+
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.")
	case "test2":
		bigString := "This is a long string sent in the request. " +
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
			"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
		resp, err := client.SimpleCall(r.Context(), &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%s", resp.Data)
	}
}

func app2Logic(w http.ResponseWriter, r *http.Request) {
	switch testVar {
	case "test":
		time.Sleep(10 * time.Millisecond)
		fmt.Fprintf(w, "This is a long string returned by the server. "+
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. "+
			"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "+
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.")
	case "test2":
		bigString := "This is a long string sent in the request. " +
			"Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
			"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
		resp, err := client2.SimpleCall2(r.Context(), &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%s", resp.Data)
	}
}
