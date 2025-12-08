package oteltool

import (
	"context"
	"fmt"
	"test/utils"
	"time"

	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/stats/opentelemetry"
)

var logger = grpclog.Component("oteltool")

// Initialize a gRPC connection to be used by both the tracer and meter
// providers.
func initConn() (*grpc.ClientConn, error) {
	// It connects the OpenTelemetry Collector through local gRPC connection.
	// You may replace `localhost:4317` with your endpoint.
	otelAddr := utils.GetEnvVar("OTEL_ADDR", true)
	conn, err := grpc.NewClient(otelAddr,
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	return conn, err
}

// Initializes an OTLP exporter, and configures the corresponding trace provider.
func initTracerProvider(ctx context.Context, res *resource.Resource,
	conn *grpc.ClientConn, frontend bool) (func(context.Context) error, error) {
	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter,
		sdktrace.WithBatchTimeout(10*time.Second))

	var sampler sdktrace.Sampler
	if frontend {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(utils.StrToFloat64(utils.GetEnvVar("SAMPLE_RATE", true))))
	} else {
		sampler = sdktrace.ParentBased(
			sdktrace.NeverSample(),
			sdktrace.WithRemoteParentSampled(sdktrace.AlwaysSample()),   // Sampler for remote sampled parent
			sdktrace.WithRemoteParentNotSampled(sdktrace.NeverSample()), // Sampler for remote unsampled parent
			sdktrace.WithLocalParentSampled(sdktrace.AlwaysSample()),    // Sampler for local sampled parent
			sdktrace.WithLocalParentNotSampled(sdktrace.NeverSample()),  // Sampler for local unsampled parent
		)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)

	// Set global propagator to tracecontext (the default is no-op).
	// Note that we are using b3 instead of tracecontext (w3c) here because
	// envoy does not support tracecontext yet.
	otel.SetTextMapPropagator(b3.New())

	// Shutdown will flush any remaining spans and shut down the exporter.
	return tracerProvider.Shutdown, nil
}

// Initializes an OTLP exporter, and configures the corresponding meter provider.
func initMeterProvider(ctx context.Context, res *resource.Resource,
	conn *grpc.ClientConn) (func(context.Context) error, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(100*time.Millisecond))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	return meterProvider.Shutdown, nil
}

// Initializes all required providers and returns their shutdown callbacks
func InitializeOTel(ctx context.Context, serviceName string, frontend bool) ([]func(context.Context) error, bool) {
	shutdownList := make([]func(context.Context) error, 0)

	// OpenTelemetry configuration
	otel_conn, err := initConn()
	if err != nil {
		logger.Error(err)
		return nil, false
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// The service name used to display traces in backends
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		logger.Error(err)
		return nil, false
	}
	if ((utils.GetEnvVar("sidecar", false) == "true") && (utils.GetEnvVar("queuing_export", false) == "true")) ||
		(utils.GetEnvVar("rajomon", false) == "true") || (utils.GetEnvVar("dagor", false) == "true") ||
		(utils.GetEnvVar("plain", false) == "true") {
		shutdownMeterProvider, err := initMeterProvider(ctx, res, otel_conn)
		if err != nil {
			logger.Error(err)
			return nil, false
		}
		shutdownList = append(shutdownList, shutdownMeterProvider)
	}

	shutdownTracerProvider, err := initTracerProvider(ctx, res, otel_conn, frontend)
	if err != nil {
		logger.Error(err)
		return nil, false
	}
	shutdownList = append(shutdownList, shutdownTracerProvider)

	return shutdownList, true
}

func CreateHistogram(meter metric.Meter, name string, description string, unit string, buckets []float64) metric.Float64Histogram {
	if hist, err := meter.Float64Histogram(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
		metric.WithExplicitBucketBoundaries(buckets...),
	); err != nil {
		logger.Fatalf("failed to create histogram: %s", err)
		return nil
	} else {
		return hist
	}
}

func CreateCounter(meter metric.Meter, name string, description string, unit string) metric.Int64Counter {
	if counter, err := meter.Int64Counter(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit)); err != nil {
		logger.Fatalf("failed to create counter: %s", err)
		return nil
	} else {
		return counter
	}
}

func CreateGauge(meter metric.Meter, name string, description string, unit string) metric.Int64Gauge {
	if gauge, err := meter.Int64Gauge(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit)); err != nil {
		logger.Fatalf("failed to create gauge: %s", err)
		return nil
	} else {
		return gauge
	}
}

func ConfigOTL(ctx context.Context, serviceName string) (grpc.ServerOption, []func(context.Context) error, bool, trace.Tracer) {
	if shutdownList, ok := InitializeOTel(ctx, serviceName, false); ok {
		tracer := otel.GetTracerProvider().Tracer(serviceName + "-tracer")
		//meter = otel.GetMeterProvider().Meter(serviceName + "-meter")
		return opentelemetry.ServerOption(opentelemetry.Options{
			MetricsOptions: opentelemetry.MetricsOptions{MeterProvider: otel.GetMeterProvider()}}), shutdownList, true, tracer
	} else {
		return nil, nil, false, nil
	}

}
