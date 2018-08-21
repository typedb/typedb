/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package brave.grpc;

import brave.Span;
import brave.Tracer;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Injector;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.function.Supplier;

/**
 * Testing thread hopping using a hack here
 */

public class CustomTracingClientInterceptor implements ClientInterceptor {
    static final Setter<Metadata, Metadata.Key<String>> SETTER =
            new Setter<Metadata, Metadata.Key<String>>() { // retrolambda no like
                @Override public void put(Metadata metadata, Metadata.Key<String> key, String value) {
                    metadata.removeAll(key);
                    metadata.put(key, value);
                }

                @Override public String toString() {
                    return "Metadata::put";
                }
            };

    final Tracer tracer;
    final Injector<Metadata> injector;
    final GrpcClientParser parser;

    private Supplier<TraceContext> getTraceContext;

    public CustomTracingClientInterceptor(GrpcTracing grpcTracing, Supplier<TraceContext> supplier) {
        tracer = grpcTracing.tracing.tracer();
        injector = grpcTracing.propagation.injector(SETTER);
        parser = grpcTracing.clientParser;
        getTraceContext = supplier;
    }

    /**
     * This sets as span in scope both for the interception and for the start of the request. It does
     * not set a span in scope during the response listener as it is unexpected it would be used at
     * that fine granularity. If users want access to the span in a response listener, they will need
     * to wrap the executor with one that's aware of the current context.
     */
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall (
            final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions,
            final Channel next) {

//        TraceContext traceContext = getTraceContext.get();
//        System.out.println("TraceContext: ");
//        System.out.println(traceContext);
//
//        Span span;
//        if (traceContext == null) {
//            span = tracer.nextSpan();
//        } else {
//            span = tracer.joinSpan(traceContext);
//        }

//        Span span = tracer.nextSpan();
//        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {


                // helper method to obtain span or make a new one
                private Span getSpan() {
                    TraceContext traceContext = getTraceContext.get();
                    System.out.println("TraceContext: ");
                    System.out.println(traceContext);

                    Span span;
                    if (traceContext == null) {
                        span = tracer.nextSpan();
                    } else {
                        span = tracer.joinSpan(traceContext);
                    }
                    return span;
                }

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    injector.inject(span.context(), headers);
                    span.kind(Span.Kind.CLIENT).start();
                    try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
                        parser.onStart(method, callOptions, headers, span.customizer());
                        super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
                            @Override public void onMessage(RespT message) {
                                try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
                                    parser.onMessageReceived(message, span.customizer());
                                    delegate().onMessage(message);
                                }
                            }

                            @Override public void onClose(Status status, Metadata trailers) {
                                try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
                                    super.onClose(status, trailers);
                                    parser.onClose(status, trailers, span.customizer());
                                } finally {
                                    span.finish();
                                }
                            }
                        }, headers);
                    }
                }

                @Override public void sendMessage(ReqT message) {

                    try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
                        super.sendMessage(message);
                        parser.onMessageSent(message, span.customizer());
                    }
                }
            };
//        } catch (RuntimeException | Error e) {
//            span.error(e).finish();
//            throw e;
//        }
    }
}

