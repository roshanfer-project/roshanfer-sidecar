package main

import (
	"context"
	"fmt"
	"net"
	"test"
	oteltool "test/otel_tool"
	"test/protobuf"
	"test/utils"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

const serviceName = "backend2"

var log = utils.GetLogger(serviceName)
var backend2repeat int

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

func init() {
	backend2repeat = utils.StrToInt(utils.GetEnvVar("backend2Repeat", true))
}

type Backend2Server struct {
	protobuf.UnimplementedBackend2Server
}

func (s *Backend2Server) SimpleCall(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	busyLoop(backend2repeat) // simulate some processing delay
	resp := &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}
	return resp, nil
}

func (s *Backend2Server) SimpleCall2(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	busyLoop(backend2repeat) // simulate some processing delay
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

func (s *Backend2Server) Run() error {

	opts := test.Opts

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
	protobuf.RegisterBackend2Server(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", utils.StrToInt(utils.GetEnvVar("Backend2ListenPort", true))))
	if err != nil {
		log.Error(fmt.Sprintf("failed to listen: %v", err))
	}

	return srv.Serve(lis)
}

func main() {
	s := &Backend2Server{}
	log.Info("Starting backend2 server")
	if err := s.Run(); err != nil {
		log.Error("main", "failed to run backend2 server", err)
	}
}
