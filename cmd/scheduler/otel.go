package main

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Configura o SDK OpenTelemetry com exportadores OTLP para rastreamento, métricas e logs.
func setupOTelSDK(ctx context.Context) (func(context.Context) error, error) {
	var shutdownFuncs []func(context.Context) error
	var err error
	// shutdown executa todas as funções de desligamento registradas.
	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}
	// lida com erros durante a configuração, garantindo que os recursos sejam liberados.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}
	// inicia o propagador global.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)
	// configura o provedor de rastreamento.
	tracerProvider, err := newTracerProvider(ctx)
	if err != nil {
		handleErr(err)
		return shutdown, err
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)
	// configura o provedor de métricas.
	meterProvider, err := newMeterProvider(ctx)
	if err != nil {
		handleErr(err)
		return shutdown, err
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)
	// configura o provedor de logs.
	loggerProvider, err := newLoggerProvider(ctx)
	if err != nil {
		handleErr(err)
		return shutdown, err
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)
	return shutdown, err
}

// Cria um propagador composto para contexto de rastreamento e bagagem.
func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// Cria um provedor de rastreamento com exportador OTLP via gRPC.
func newTracerProvider(ctx context.Context) (*trace.TracerProvider, error) {
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter, trace.WithBatchTimeout(5*time.Second)),
	)
	return tracerProvider, nil
}

// Cria um provedor de métricas com exportador OTLP via gRPC.
func newMeterProvider(ctx context.Context) (*metric.MeterProvider, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(5*time.Second))),
	)
	return meterProvider, nil
}

// Cria um provedor de logs com exportador OTLP via gRPC.
func newLoggerProvider(ctx context.Context) (*log.LoggerProvider, error) {
	// configura o log para sysout
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)
	return loggerProvider, nil
}
