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

import static brave.internal.HexCodec.toLowerHex;


/**
 * Custom gRPC client-side message interceptor that utilizes contexts per message, and calculates
 * time taken on the server close to the network (in gRPC thread, rather than the user thread)
 * This should allow us to see, at some point, if we spend a lot of time in a queue somewhere
 */
public class GrpcClientInterceptor implements ClientInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcClientInterceptor.class);
    private Tracer tracer;


    public GrpcClientInterceptor(Tracing tracing) {
        this.tracer = tracing.tracer();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {

        // this is called once per gRPC endpoint call
        // the SimpleForwardingClientCall does the work on intercepting messages/responses


        /*
            One of these is created per call to transaction()
            Therefore thread safe under the assumption of only 1 message in flight
            per transaction at a time
         */
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {


            Span currentClientSpan = tracer.nextSpan(); //initialize to pass compilation, then abandon it
            int childMsgNumber = 0;


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
                if (message instanceof SessionProto.Transaction.Req &&
                        ((SessionProto.Transaction.Req) message).
                                getMetadataOrDefault("traceIdLow", "").
                                length() > 0) {
                    SessionProto.Transaction.Req txReq = (SessionProto.Transaction.Req) message;
                    String traceIdHigh = txReq.getMetadataOrThrow("traceIdHigh");
                    String traceIdLow = txReq.getMetadataOrThrow("traceIdLow");
                    String spanId = txReq.getMetadataOrThrow("spanId");
                    String parentId = txReq.getMetadataOrDefault("parentId", "");

                    String msgField= txReq.getReqCase().name();

                    TraceContext reconstructedContext = GrpcMessageConversion.stringsToContext(traceIdHigh, traceIdLow, spanId, parentId);
                    currentClientSpan = tracer.newChild(reconstructedContext).name("Client: " + msgField);
                    currentClientSpan.start();
                    if (LOG.isDebugEnabled()) {
                        currentClientSpan.tag("sendMessage", message.toString());
                    }

                    currentClientSpan.tag("childNumber", Integer.toString(childMsgNumber));
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
