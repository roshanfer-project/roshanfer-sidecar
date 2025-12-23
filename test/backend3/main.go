package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"test"
	oteltool "test/otel_tool"
	"test/protobuf"
	"test/utils"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

const serviceName = "backend3"

var log = utils.GetLogger(serviceName)
var backend3repeat int

func configOTL(ctx context.Context, serviceName string) (grpc.ServerOption, []func(context.Context) error, bool) {
	if shutdownList, ok := oteltool.InitializeOTel(ctx, serviceName, false); ok {
		return opentelemetry.ServerOption(opentelemetry.Options{
			MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()}}), shutdownList, true
	} else {
		return nil, nil, false
	}
}

func init() {
	backend3repeat = utils.StrToInt(utils.GetEnvVar("backend3Repeat", true))
	if bl := utils.GetEnvVar("BUSY_LOOP", false); bl != "" {
		backend3repeat = utils.StrToInt(bl)
	}

	// Check for service-specific repeat
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName != "" {
		if r := utils.GetEnvVar(serviceName+"Repeat", false); r != "" {
			backend3repeat = utils.StrToInt(r)
		}
	}
}

type Backend3Server struct {
	protobuf.UnimplementedBackend3Server
}

func (s *Backend3Server) SimpleCall(ctx context.Context, req *protobuf.Arg3) (*protobuf.Resp3, error) {
	busyLoop(backend3repeat) // simulate some processing delay
	resp := &protobuf.Resp3{
		Data: "Hello from Backend3, " + req.Data,
	}
	return resp, nil
}

func busyLoop(repeat int) {
	for range repeat {
		for range 10000 {
		}
	}
}

func (s *Backend3Server) Run() error {

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
	protobuf.RegisterBackend3Server(srv, s)

	port := utils.StrToInt(utils.GetEnvVar("Backend3ListenPort", true))
	if p := utils.GetEnvVar("PORT", false); p != "" {
		port = utils.StrToInt(p)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Error(fmt.Sprintf("failed to listen: %v", err))
	}

	return srv.Serve(lis)
}

func main() {
	s := &Backend3Server{}
	log.Info("Starting backend3 server")
	if err := s.Run(); err != nil {
		log.Error("main", "failed to run backend3 server", err)
	}
}
