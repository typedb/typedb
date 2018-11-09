package ai.grakn.core.server.benchmark;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import ai.grakn.rpc.proto.SessionProto;


/**
 * This component is stateless, as parts of it (eg initInstrumentation) is only called once
 * whereas the others may be used my multiple threads in the case of concurrent transactions
 */
public class ServerTracingInstrumentation {

    public static void initInstrumentation(String tracingServiceName) {
        // create a Zipkin reporter for the whole server
        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));

        // create a global Tracing instance with reporting
        Tracing.newBuilder()
                .localServiceName(tracingServiceName)
                .supportsJoin(false)
                .spanReporter(reporter)
                .build();
    }


    /**
     * Determine if tracing is enabled at all on the server
     * @return
     */
    public static boolean tracingEnabled() {
        return Tracing.currentTracer() != null;
    }

    /**
     * Retrieves the current active Span (thread-local)
     * @return
     */
    public static Span currentSpan() {
        return Tracing.currentTracer().currentSpan();
    }

    /**
     * Determine if tracing is enabled on the server and there is a thread-local active span
     */
    public static boolean existsCurrentSpan() {
        return tracingEnabled() && currentSpan() != null;
    }

    /**
     * Determine if tracing is enabled on the server
     * and the received message contains a TraceContext transmitted in the metadata fields
     * @param message
     * @return
     */
    public static boolean tracingEnabledFromMessage(SessionProto.Transaction.Req message) {
        if (tracingEnabled() && message.getMetadataOrDefault("traceIdLow", "").length() > 0) {
            return true;
        }
        return false;
    }

    public static TraceContext extractTraceContext(SessionProto.Transaction.Req message) {
        String traceIdHigh = message.getMetadataOrThrow("traceIdHigh");
        String traceIdLow = message.getMetadataOrThrow("traceIdLow");
        String spanId = message.getMetadataOrThrow("spanId");
        String parentId = message.getMetadataOrDefault("parentId", "");
        return GrpcMessageConversion.stringsToContext(traceIdHigh, traceIdLow, spanId, parentId);
    }



    /**
     *
     * @param spanName
     * @param parentContext
     * @return A new Span with the given parent Context, NOT thread-local and NOT `.start()`-ed
     */
    public static Span createChildSpan(String spanName, TraceContext parentContext) {
        Tracer tracing = Tracing.currentTracer();
        Span child = tracing.newChild(parentContext);
        child.name(spanName);
        return child;
    }

    /**
     *
     * @param spanName
     * @param parentContext
     * @return A new ScopedSpan with the given parent Context (ie. thread-local and `.start()` already has been called)
     */
    public static ScopedSpan startScopedChildSpan(String spanName, TraceContext parentContext) {
        Tracer tracing = Tracing.currentTracer();
        ScopedSpan child = tracing.startScopedSpanWithParent(spanName, parentContext);
        return child;
    }
}
