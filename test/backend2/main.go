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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats/opentelemetry"
)

const serviceName = "backend2"

var log = utils.GetLogger(serviceName)
var client protobuf.Backend3Client

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

type Backend2Server struct {
	protobuf.UnimplementedBackend2Server
}

func (s *Backend2Server) SimpleCall(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	processingLogic(ctx)
	if client != nil {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		// Convert Arg to Arg3
		arg3 := &protobuf.Arg3{Data: req.Data}
		resp3, err := client.SimpleCall(ctx, arg3)
		if err != nil {
			return nil, err
		}
		return &protobuf.Resp{Data: resp3.Data}, nil
	}
	resp := &protobuf.Resp{
		Data: "Hello, " + req.Data,
	}
	return resp, nil
}

func (s *Backend2Server) SimpleCall2(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	processingLogic(ctx)
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

func processingLogic(ctx context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		panic("metadata not present")
	}
	api, ok := md["api"]
	if !ok {
		panic("api is nil")
	}
	if len(api) != 1 {
		panic(fmt.Sprintf("api: %+v is invalid", api))
	}

	repeat := utils.StrToInt(utils.GetEnvVar(api[0]+serviceName+"Repeat", true))
	busyLoop(repeat)
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

	port := utils.StrToInt(utils.GetEnvVar("Backend2ListenPort", true))
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
	// Check for chaining
	nextHop := utils.GetEnvVar("NEXT_HOP", false)
	if utils.GetEnvVar("sidecar", false) == "true" {
		if val := utils.GetEnvVar("Backend3_BE2_Egress", false); val != "" {
			nextHop = val
		}
	}
	if nextHop != "" {
		conn := test.GetConn(nextHop)
		client = protobuf.NewBackend3Client(conn)
		log.Info("Chaining enabled", "next_hop", nextHop)
	}

	s := &Backend2Server{}
	log.Info("Starting backend2 server")
	if err := s.Run(); err != nil {
		log.Error("main", "failed to run backend2 server", err)
	}
}
