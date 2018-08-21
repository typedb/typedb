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

import ai.grakn.rpc.proto.SessionProto;
import brave.Span;
import brave.Tracer;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
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

import static brave.internal.HexCodec.lowerHexToUnsignedLong;

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

    public CustomTracingClientInterceptor(GrpcTracing grpcTracing) {
        tracer = grpcTracing.tracing.tracer();
        injector = grpcTracing.propagation.injector(SETTER);
        parser = grpcTracing.clientParser;
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

        // NOTE this is called *once* per transaction() or keyspace() etc.
        // NOT per-message
        // but the onMessage and sendMessage methods below ARE called

        Span span = tracer.nextSpan();
        span.name("Client RPC call");
        span.kind(Span.Kind.CLIENT);

        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
            return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

                private TraceContext stringsToContext(String traceIdHighStr,
                                                      String traceIdLowStr,
                                                      String spanIdStr,
                                                      String parentIdStr) {
                    long traceIdHigh = lowerHexToUnsignedLong(traceIdHighStr);
                    long traceIdLow = lowerHexToUnsignedLong(traceIdLowStr);
                    long spanId = lowerHexToUnsignedLong(spanIdStr);
                    Long parentId;
                    if (parentIdStr.length() == 0) {
                        parentId = null;
                    } else {
                        parentId = lowerHexToUnsignedLong(parentIdStr);
                    }
                    return constructContext(traceIdHigh, traceIdLow, spanId, parentId);
                }

                // helper method to obtain span or make a new one
                private TraceContext constructContext(long traceIdHigh,
                                                      long traceIdLow,
                                                      long spanId,
                                                      Long parentId) {
                    TraceContext.Builder builder = TraceContext.newBuilder()
                                                        .traceIdHigh(traceIdHigh)
                                                        .traceId(traceIdLow)
                                                        .spanId(spanId)
                                                        .parentId(parentId);
                    TraceContext context = builder.build();
                    return context;
                }

//                TODO integrate these
//                TraceContextOrSamplingFlags traceContextOrSamplingFlags = TraceContextOrSamplingFlags.create(context);
//                Span span = tracer.nextSpan(traceContextOrSamplingFlags);
//                    return span;

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
                        @Override public void onMessage(RespT message) {
                            Span span;
                            if (message instanceof SessionProto.Transaction.Res &&
                                    ((SessionProto.Transaction.Res)message).
                                            getMetadataOrDefault("traceIdHigh", "").
                                            length() > 0) {
                                SessionProto.Transaction.Res txRes = (SessionProto.Transaction.Res) message;
                                String traceIdHigh = txRes.getMetadataOrThrow("traceIdHigh");
                                String traceIdLow = txRes.getMetadataOrThrow("traceIdLow");
                                String spanId = txRes.getMetadataOrThrow("spanId");
                                String parentId = txRes.getMetadataOrDefault("parentId", "");

                                TraceContext reconstructedContext = stringsToContext(traceIdHigh, traceIdLow, spanId, parentId);
                                span = tracer.newChild(reconstructedContext).kind(Span.Kind.CLIENT);
                            } else {
                                System.out.println("Unimplemented message type in clientInterceptor.sendMessage");
                                span = tracer.nextSpan().kind(Span.Kind.CLIENT);
                            }

                            // handle this asynchronously
                            span.start();
                            span.annotate("Client receive response");
                            parser.onMessageReceived(message, span.customizer());
                            delegate().onMessage(message);
                            span.flush();
                        }

                        @Override public void onClose(Status status, Metadata trailers) {
                            super.onClose(status, trailers);
                            parser.onClose(status, trailers, span.customizer());
                        }
                    }, headers);
                }

                @Override public void sendMessage(ReqT message) {
                    // try casting it to grpc message types
                    Span span;
                    if (message instanceof SessionProto.Transaction.Req &&
                            ((SessionProto.Transaction.Req)message).
                                    getMetadataOrDefault("traceIdHigh", "").
                                    length() > 0) {
                        SessionProto.Transaction.Req txReq = (SessionProto.Transaction.Req) message;
                        String traceIdHigh = txReq.getMetadataOrThrow("traceIdHigh");
                        String traceIdLow = txReq.getMetadataOrThrow("traceIdLow");
                        String spanId = txReq.getMetadataOrThrow("spanId");
                        String parentId = txReq.getMetadataOrDefault("parentId", "");

                        TraceContext reconstructedContext = stringsToContext(traceIdHigh, traceIdLow, spanId, parentId);
                        span = tracer.newChild(reconstructedContext).kind(Span.Kind.CLIENT);
                    } else {
                        System.out.println("Unimplemented message type in clientInterceptor.sendMessage");
                        span = tracer.nextSpan().kind(Span.Kind.CLIENT);
                    }

                    // handle this asynchronously
                    span.start();
                    span.annotate("Client send request");
                    super.sendMessage(message);
                    parser.onMessageSent(message, span.customizer());
                    span.flush();
                }
            };
        }
    }
}

