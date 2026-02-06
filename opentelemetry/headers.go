package opentelemetry

import (
	"context"

	"go.opentelemetry.io/otel"
)

type MessageHeadersCarrier map[string]interface{}

func (c MessageHeadersCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	return v.(string)
}

func (c MessageHeadersCarrier) Set(key string, value string) {
	c[key] = value
}

func (c MessageHeadersCarrier) Keys() []string {
	var i int
	keys := make([]string, len(c))
	for k := range c {
		keys[i] = k
		i++
	}
	return keys
}

// InjectTraceIntoMessageHeader injects the tracing headers from the context into the header map.
func InjectTraceIntoMessageHeader(ctx context.Context, headers map[string]interface{}) map[string]interface{} {
	h := MessageHeadersCarrier(headers)
	otel.GetTextMapPropagator().Inject(ctx, h)
	return h
}

// ExtractTraceFromMessageHeader extracts the tracing from the header and puts it into the context.
func ExtractTraceFromMessageHeader(ctx context.Context, headers map[string]interface{}) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, MessageHeadersCarrier(headers))
}
