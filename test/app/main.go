// This is a simple http server

package main

import (
	"context"
	"fmt"
	"net/http"

	_ "net/http/pprof"
	"strconv"
	"strings"
	"test"
	"test/protobuf"
	"test/utils"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

var deployment string
var appSize int
var lastRpcId int

// var app2Size int
var client protobuf.Backend1Client

// var client2 protobuf.Backend1Client
var clientBackend2 protobuf.Backend2Client

// var client2Backend2 protobuf.Backend2Client
var appPreRepeat int
var appPostRepeat int

var listenPort int
var sidecar bool
var tracer trace.Tracer

// var app2PreRepeat int
// var app2PostRepeat int

var log = utils.GetLogger("app")

func tracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := getContextWithRpcId(r)
		ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func main() {
	// Configure OTL
	ctx := context.Background()
	test.ConfigOTL(ctx, "app", true)
	tracer = otel.GetTracerProvider().Tracer("app-tracer")

	mux := http.NewServeMux()
	/* http.HandleFunc("/app", func(w http.ResponseWriter, r *http.Request) {
		appLogic(w, getContextWithRpcId(r))
	}) */
	mux.Handle("/app", tracingMiddleware(http.HandlerFunc(appLogic)))

	// Start pprof server
	go func() {
		pprofPort := 6000
		log.Info("Starting pprof server", "port", pprofPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", pprofPort), nil); err != nil {
			log.Error("pprof server failed", "error", err)
		}
	}()

	/* http.HandleFunc("/app2", func(w http.ResponseWriter, r *http.Request) {
		app2Logic(w, getContextWithRpcId(r))
	}) */
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", listenPort),
		Handler: mux,
	}
	log.Info("Starting server on port", "listenPort", listenPort)
	srv.ListenAndServe()
}

func getContextWithRpcId(r *http.Request) context.Context {
	var rpcId string
	if sidecar {
		rpcId = r.Header.Get("rpc-id")
		/* if rpcId == "" {
			log.Error("rpc-id header is required")
			return r.Context()
		} */
	} else {
		rpcId = strconv.Itoa(lastRpcId)
		r.Header.Set("rpc-id", rpcId)
		lastRpcId++
	}
	md := metadata.New(map[string]string{"rpc-id": rpcId})
	return metadata.NewOutgoingContext(r.Context(), md)
}

func init() {
	lastRpcId = 1
	deployment = utils.GetEnvVar("deployment", true)
	sidecar = utils.GetEnvVar("sidecar", true) == "true"
	listenPort = utils.StrToInt(utils.GetEnvVar("appListenPort", true))
	appSize = utils.StrToInt(utils.GetEnvVar("appSize", true))
	// app2Size = utils.StrToInt(utils.GetEnvVar("app2Size", true))
	appPreRepeat = utils.StrToInt(utils.GetEnvVar("appPreRepeat", true))
	appPostRepeat = utils.StrToInt(utils.GetEnvVar("appPostRepeat", true))
	// app2PreRepeat = utils.StrToInt(utils.GetEnvVar("app2PreRepeat", true))
	// app2PostRepeat = utils.StrToInt(utils.GetEnvVar("app2PostRepeat", true))
	fmt.Printf("deployment: %s\n", deployment)
	fmt.Printf("appSize: %d\n", appSize)
	switch deployment {
	case "test2":
		conn := test.GetConn(utils.GetEnvVar("Backend1AppEgress", true))
		client = protobuf.NewBackend1Client(conn)
		// conn = test.GetConn(utils.GetEnvVar("AppEgress", true))
		// client2 = protobuf.NewBackend1Client(conn)
	case "test3":
		conn := test.GetConn(utils.GetEnvVar("Backend1AppEgress", true))
		client = protobuf.NewBackend1Client(conn)
		// conn = test.GetConn(utils.GetEnvVar("AppEgress", true))
		// client2 = protobuf.NewBackend1Client(conn)
		conn = test.GetConn(utils.GetEnvVar("Backend2AppEgress", true))
		clientBackend2 = protobuf.NewBackend2Client(conn)

		// conn = test.GetConn(utils.GetEnvVar("AppEgress", true))
		// clientBackend2 = protobuf.NewBackend2Client(conn)
		// conn = test.GetConn(utils.GetEnvVar("AppEgress", true))
		// client2Backend2 = protobuf.NewBackend2Client(conn)
	}
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

func busyLoopDuration(duration_us int) {
	spent_us := 0
	var now time.Time
	for spent_us < duration_us {
		now = time.Now()
		busyLoop(1)
		spent_us += int(time.Since(now).Microseconds())
	}
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
	ctx := r.Context()
	switch deployment {
	case "test1":
		busyLoop(appPreRepeat + appPostRepeat)
		writeResponseWithoutchunkEncoding(w, makebigString(appSize))
	case "test2":
		_, span := tracer.Start(ctx, "pre-processing")
		busyLoop(appPreRepeat)
		bigString := makebigString(appSize)
		span.End()
		childCtx, childSpan := tracer.Start(ctx, "grpc-call", trace.WithAttributes(attribute.String("rpc-id", r.Header.Get("rpc-id"))))
		resp, err := client.SimpleCall(childCtx, &protobuf.Arg{Data: bigString})
		childSpan.End()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, span = tracer.Start(ctx, "post-processing")
		busyLoop(appPostRepeat)
		span.End()
		writeResponseWithoutchunkEncoding(w, resp.Data)
	case "test3":
		busyLoop(appPreRepeat)
		bigString := makebigString(appSize)
		_, err := client.SimpleCall(ctx, &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		busyLoop(appPostRepeat)
		resp, err := clientBackend2.SimpleCall(ctx, &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		busyLoop(appPostRepeat)
		writeResponseWithoutchunkEncoding(w, resp.Data)
	}
}

/* func app2Logic(w http.ResponseWriter, ctx context.Context) {
	switch testVar {
	case "test":
		time.Sleep(time.Duration(app2Time) * time.Millisecond)
		writeResponseWithoutchunkEncoding(w, makebigString(app2Size))
	case "test2":
		time.Sleep(time.Duration(appTime) * time.Millisecond)
		bigString := makebigString(app2Size)
		resp, err := client2.SimpleCall2(ctx, &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeResponseWithoutchunkEncoding(w, resp.Data)
	case "test3":
		bigString := makebigString(app2Size)
		_, err := client2.SimpleCall2(ctx, &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		time.Sleep(time.Duration(app2Time) * time.Millisecond)
		resp, err := client2Backend2.SimpleCall2(ctx, &protobuf.Arg{Data: bigString})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeResponseWithoutchunkEncoding(w, resp.Data)
	}
} */
