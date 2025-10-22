package test

import (
	"context"
	oteltool "test/otel_tool"
	"test/utils"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats/opentelemetry"
)

var logger = utils.GetLogger("grpc-client")

// Config OpenTelemetry exporter and return a Dial option to be used by
// gRPC client
func configOTL(ctx context.Context, clientName string, frontend bool) (grpc.DialOption, []func(context.Context) error, bool) {

	if shutdownList, ok := oteltool.InitializeOTel(ctx, clientName, frontend); ok {
		return opentelemetry.DialOption(opentelemetry.Options{
			MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()}}), shutdownList, true
	} else {
		return nil, nil, false
	}

}

func ConfigOTL(ctx context.Context, clientName string, frontend bool) []func(context.Context) error {
	if _, shutdownList, ok := configOTL(ctx, clientName, frontend); ok {
		logger.Info("Successfully initialized OpenTelemetry")
		return shutdownList
	} else {
		panic("Failed to initialize OpenTelemetry for " + clientName)
	}
}

func GetConn(serverAddr string, ops ...grpc.DialOption) *grpc.ClientConn {
	// basic dial options
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()), // this is for tracing and metrics,
	}

	options = append(options, ops...)

	conn, err := grpc.NewClient(serverAddr, options...)
	if err != nil {
		panic("did not connect: " + err.Error())
	}
	return conn
}

func GetRajomonClient(serverAddr string, interceptor grpc.DialOption) *grpc.ClientConn {
	// basic dial options
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()), // this is for tracing and metrics,
		interceptor,
	}

	conn, err := grpc.NewClient(serverAddr, options...)
	if err != nil {
		panic("did not connect: " + err.Error())
	}
	return conn
}

/* func GetTierClient(conn *grpc.ClientConn) tier.TierClient {
	return tier.NewTierClient(conn)
} */

func GetConnBasic(serverAddr string) *grpc.ClientConn {
	// basic dial options
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(serverAddr, options...)
	if err != nil {
		panic("did not connect: " + err.Error())
	}
	return conn
}
