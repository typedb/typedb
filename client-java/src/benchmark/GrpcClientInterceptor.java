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

package ai.grakn.client.benchmark;

import ai.grakn.rpc.proto.SessionProto;
import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.TraceContext;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static brave.internal.HexCodec.toLowerHex;


/**
 *
 * A Grakn-specific client-side interceptor that we can attach to client-java, transparently
 * handling things such as passing Tracing contexts to the server (using the metadata field in our proto messages)
 * and recording the time taken for individual message requests as individual child spans
 *
 * Target functionality:
 * 1) record round-trip time taken for individual messages/requests sent to the server
 * 2) pass tracing context to the server to reconstruct & join the current in progress-trace
 * recall that AsyncReporters report traces to Zipkin out of band (ie. async and independently)
 *
 */
public class GrpcClientInterceptor implements ClientInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcClientInterceptor.class);
    private Tracer tracer;

    public GrpcClientInterceptor(String tracingServiceName) {

        // create a Zipkin reporter for the client
        AsyncReporter<zipkin2.Span> reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));

        // create a thread-local Tracing instance
        Tracing tracing = Tracing.newBuilder()
            .localServiceName(tracingServiceName)
            .spanReporter(reporter)
            .supportsJoin(true)
            .build();

        // save this tracer
        this.tracer = tracing.tracer();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {

        // this is called once per gRPC endpoint call
        // the SimpleForwardingClientCall does the work on intercepting messages/responses
        // creating child spans for each message that is sent across the network, even if use calls something
        // transparent like `.execute()` rather than timing each retrieval off the stream manually


        /*
            One of these is created per call to transaction()
            Therefore thread safe under the assumption of only 1 message in flight
            per transaction at a time
         */
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            Span currentClientSpan = tracer.nextSpan(); //initialize to pass compilation, then abandon it
            long childMsgNumber = 0; // for ordering at later point

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onMessage(RespT message) {
                        if (currentClientSpan != null) {
                            currentClientSpan.annotate("Client recv resp");
                            if (LOG.isDebugEnabled()) {
                                currentClientSpan.tag("receiveMessage", message.toString());
                            }
                            currentClientSpan.finish();
                            currentClientSpan = null; // null out after finishing
                        } else {
                            LOG.debug("Ignoring response type in clientInterceptor.onMessage because no active client side span: ");
                            LOG.debug("\t'" + message.getClass() + "'");
                        }
                        delegate().onMessage(message);
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        super.onClose(status, trailers);
                    }
                }, headers);
            }

            @Override
            public void sendMessage(ReqT message) {
                if (message instanceof SessionProto.Transaction.Req && tracer.currentSpan() != null) {
                    SessionProto.Transaction.Req txReq = (SessionProto.Transaction.Req) message;

                    String msgField= txReq.getReqCase().name();

                    currentClientSpan = tracer.newChild(tracer.currentSpan().context()).name("client: " + msgField);
                    currentClientSpan.start();
                    if (LOG.isDebugEnabled()) {
                        // if verbose, attach message
                        currentClientSpan.tag("gRPC message sent: ", message.toString());
                    }

                    currentClientSpan.tag("childNumber", Long.toString(childMsgNumber));
                    childMsgNumber++;

                    // --- re-pack the message with the child's data ---
                    SessionProto.Transaction.Req.Builder builder = txReq.toBuilder();
                    TraceContext childContext = currentClientSpan.context();

                    // span ID
                    String spanIdStr = toLowerHex(childContext.spanId());
                    builder.putMetadata("spanId", spanIdStr);

                    // parent ID
                    Long newParentId = childContext.parentId();
                    if (newParentId == null) {
                        builder.putMetadata("parentId", "");
                    } else {
                        builder.putMetadata("parentId", toLowerHex(newParentId));
                    }

                    // trace ID
                    String traceIdLow = toLowerHex(childContext.traceId());
                    String traceIdHigh = toLowerHex(childContext.traceIdHigh());
                    builder.putMetadata("traceIdLow", traceIdLow);
                    builder.putMetadata("traceIdHigh", traceIdHigh);

                    // Trace ID remains the same
                    message = (ReqT) builder.build(); // update the request
                } else {
                    LOG.debug("Ignoring tracing for message without tracing context: class -- '" + message.getClass() + "', msg --  '" + message.toString() + "'");
                }
                super.sendMessage(message);
            }
        };
    }
}
