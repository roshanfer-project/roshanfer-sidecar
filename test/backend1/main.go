package main

import (
	"context"
	"fmt"
	"net"

	//_ "net/http/pprof"
	"test"
	oteltool "test/otel_tool"
	"test/protobuf"
	"test/utils"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

const serviceName = "backend1"

var log = utils.GetLogger(serviceName)
var backend1Repeat int

//var tracer trace.Tracer

func init() {
	backend1Repeat = utils.StrToInt(utils.GetEnvVar("backend1Repeat", true))
}

func configOTL(ctx context.Context, serviceName string) (grpc.ServerOption, []func(context.Context) error, bool) {
	if shutdownList, ok := oteltool.InitializeOTel(ctx, serviceName, false); ok {
		//tracer = otel.GetTracerProvider().Tracer(serviceName + "-tracer")
		//meter = otel.GetMeterProvider().Meter(serviceName + "-meter")
		return opentelemetry.ServerOption(opentelemetry.Options{
			MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()}}), shutdownList, true
	} else {
		return nil, nil, false
	}

}

/* func tracingInterceptor(ctx context.Context, req any,
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {

	_, span := tracer.Start(ctx, info.FullMethod)
	defer span.End()
	return handler(ctx, req)

} */

type Backend1Server struct {
	protobuf.UnimplementedBackend1Server
}

func (s *Backend1Server) SimpleCall(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	busyLoop(backend1Repeat) // simulate some processing delay
	resp := &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}
	return resp, nil
}

func (s *Backend1Server) SimpleCall2(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	busyLoop(backend1Repeat) // simulate some processing delay
	return &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}, nil
}

func busyLoop(repeat int) {
	for range repeat {
		for range 10000 {
		}
	}
}

func (s *Backend1Server) Run() error {

	opts := test.Opts

	//opts = append(opts, grpc.UnaryInterceptor(tracingInterceptor))

	ctx := context.Background()
	if _, shutdownList, ok := configOTL(ctx, serviceName); ok {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))

		for _, f := range shutdownList {
			defer func() {
				if err := f(ctx); err != nil {
					log.Error("main", "failed to shutdown OpenTelemetry provider", err)
				}
			}()
		}
		log.Info("Successfully initialized OpenTelemetry")
	} else {
		log.Error("Failed to initialize OpenTelemetry")
	}

	srv := grpc.NewServer(opts...)
	protobuf.RegisterBackend1Server(srv, s)

	port := utils.StrToInt(utils.GetEnvVar("Backend1ListenPort", true))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Error(fmt.Sprintf("failed to listen: %v", err))
	}

	/* // Start pprof server
	go func() {
		pprofPort := 6001
		log.Info("Starting pprof server", "port", pprofPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", pprofPort), nil); err != nil {
			log.Error("pprof server failed", "error", err)
		}
	}() */

	return srv.Serve(lis)
}

func main() {
	s := &Backend1Server{}
	log.Info("Starting backend1 server")
	if err := s.Run(); err != nil {
		log.Error("main", "failed to run backend1 server", err)
	}
}
