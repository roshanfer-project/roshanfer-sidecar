package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"test"
	"test/protobuf"
	"test/utils"

	"google.golang.org/grpc/metadata"
)

var serviceName = "app-busy-chain"
var log = utils.GetLogger(serviceName)
var pre_repeat int
var post_repeat int
var listenPort int
var deployment string
var client protobuf.Backend1Client

func main() {
	http.HandleFunc("/app", func(w http.ResponseWriter, r *http.Request) {
		appLogic(w, getContextWithRpcId(r))
	})
	log.Info("Starting app-busy server", "listenPort", listenPort)
	http.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
}

func getContextWithRpcId(r *http.Request) context.Context {
	if deployment != "plain" {
		rpcId := r.Header.Get("rpc-id")
		if rpcId == "" {
			log.Error("rpc-id header is required")
			return r.Context()
		}
		md := metadata.New(map[string]string{"rpc-id": rpcId})
		return metadata.NewOutgoingContext(r.Context(), md)
	}
	return r.Context()
}

func makebigString(size int) string {
	return strings.Repeat("a", size)
}

func busyLoop(repeat int) {
	for range repeat {
		for range 10000 {
		}
	}
}

func writeResponseWithoutchunkEncoding(w http.ResponseWriter, data string) {
	// convert to bytes
	responseBytes := []byte(data)
	w.Header().Set("Content-Length", strconv.Itoa(len(responseBytes)))
	// write the data
	w.Write(responseBytes)

}

func appLogic(w http.ResponseWriter, ctx context.Context) {
	busyLoop(pre_repeat)
	resp, err := client.SimpleCall(ctx, &protobuf.Arg{Data: makebigString(1024)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	busyLoop(post_repeat)
	writeResponseWithoutchunkEncoding(w, resp.Data)
}

func init() {
	pre_repeat = utils.StrToInt(utils.GetEnvVar(serviceName+"-pre-repeat", true))
	post_repeat = utils.StrToInt(utils.GetEnvVar(serviceName+"-post-repeat", true))
	listenPort = utils.StrToInt(utils.GetEnvVar(serviceName+"-listen-port", true))
	deployment = utils.GetEnvVar("deployment", true)
	conn := test.GetConnBasic(utils.GetEnvVar(serviceName+"-egress", true))
	client = protobuf.NewBackend1Client(conn)
}
