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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats/opentelemetry"
)

const serviceName = "backend1"

var log = utils.GetLogger(serviceName)
var client protobuf.Backend2Client
var client3 protobuf.Backend3Client

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
	processingLogic(ctx)
	// Propagate metadata (rpc-id)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	if client != nil {
		return client.SimpleCall(ctx, req)
	}
	if client3 != nil {
		// Convert Arg to Arg3
		arg3 := &protobuf.Arg3{Data: req.Data}
		resp3, err := client3.SimpleCall(ctx, arg3)
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

func (s *Backend1Server) SimpleCall2(ctx context.Context, req *protobuf.Arg) (*protobuf.Resp, error) {
	processingLogic(ctx)
	if client != nil {
		// assuming SimpleCall2 also chains? Or maybe just SimpleCall.
		// User said "use the existing protobuf definition twise".
		// If the proto has SimpleCall, then we use SimpleCall.
		// For now, let's keep SimpleCall2 as is or chain it if needed.
		// But chain3 likely uses SimpleCall.
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		return client.SimpleCall2(ctx, req)
	}
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
	if p := utils.GetEnvVar("PORT", false); p != "" {
		port = utils.StrToInt(p)
	}

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
	// Check for chaining
	nextHop := utils.GetEnvVar("NEXT_HOP", false)
	nextHopProto := utils.GetEnvVar("NEXT_HOP_PROTO", false)

	if utils.GetEnvVar("sidecar", false) == "true" {
		if val := utils.GetEnvVar("Backend3_BE1_Egress", false); val != "" {
			nextHop = val
		}
	}

	if nextHop != "" {
		conn := test.GetConn(nextHop)
		if nextHopProto == "backend3" {
			client3 = protobuf.NewBackend3Client(conn)
			log.Info("Chaining enabled (Backend3)", "next_hop", nextHop)
		} else {
			// Default to Backend2 for backward compatibility
			client = protobuf.NewBackend2Client(conn)
			log.Info("Chaining enabled (Backend2)", "next_hop", nextHop)
		}
	}

	s := &Backend1Server{}
	log.Info("Starting backend1 server")
	if err := s.Run(); err != nil {
		log.Error("main", "failed to run backend1 server", err)
	}
}
