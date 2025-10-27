// This is a simple http server

package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"test"
	"test/protobuf"
	"test/utils"
	"time"
)

var testVar string
var appSize int
var app2Size int
var client protobuf.Backend1Client
var client2 protobuf.Backend1Client
var appTime int
var app2Time int

func main() {
	http.HandleFunc("/app", func(w http.ResponseWriter, r *http.Request) {
		appLogic(w, r)
	})
	http.HandleFunc("/app2", func(w http.ResponseWriter, r *http.Request) {
		app2Logic(w, r)
	})
	http.ListenAndServe(":2007", nil)
}

func init() {
	testVar = utils.GetEnvVar("test", true)
	appSize = utils.StrToInt(utils.GetEnvVar("appSize", true))
	app2Size = utils.StrToInt(utils.GetEnvVar("app2Size", true))
	appTime = utils.StrToInt(utils.GetEnvVar("appTime", true))
	app2Time = utils.StrToInt(utils.GetEnvVar("app2Time", true))
	fmt.Printf("testVar: %s\n", testVar)
	if testVar == "test2" {
		conn := test.GetConnBasic(utils.GetEnvVar("AppEgress", true))
		client = protobuf.NewBackend1Client(conn)
		conn = test.GetConnBasic(utils.GetEnvVar("AppEgress", true))
		client2 = protobuf.NewBackend1Client(conn)
	}
}

func makebigString(size int) string {
	return strings.Repeat("a", size)
}

func writeResponseWithoutchunkEncoding(w http.ResponseWriter, data string) {
	// convert to bytes
	responseBytes := []byte(data)
	w.Header().Set("Content-Length", strconv.Itoa(len(responseBytes)))
	// write the data
	w.Write(responseBytes)

}

// with no chunk-encoding
func appLogic(w http.ResponseWriter, r *http.Request) {
	switch testVar {
	case "test":
		time.Sleep(time.Duration(appTime) * time.Millisecond)
		writeResponseWithoutchunkEncoding(w, makebigString(appSize))
	case "test2":
		bigString := makebigString(appSize)
		resp, err := client.SimpleCall(r.Context(), &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeResponseWithoutchunkEncoding(w, resp.Data)
	}
}

func app2Logic(w http.ResponseWriter, r *http.Request) {
	switch testVar {
	case "test":
		time.Sleep(time.Duration(app2Time) * time.Millisecond)
		writeResponseWithoutchunkEncoding(w, makebigString(app2Size))
	case "test2":
		bigString := makebigString(app2Size)
		resp, err := client2.SimpleCall2(r.Context(), &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeResponseWithoutchunkEncoding(w, resp.Data)
	}
}
