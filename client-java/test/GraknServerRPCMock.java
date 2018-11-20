/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.client.test;

import grakn.core.protocol.KeyspaceProto;
import grakn.core.protocol.KeyspaceServiceGrpc;
import grakn.core.protocol.KeyspaceServiceGrpc.KeyspaceServiceImplBase;
import grakn.core.protocol.SessionProto;
import grakn.core.protocol.SessionProto.Transaction;
import grakn.core.protocol.SessionServiceGrpc;
import grakn.core.protocol.SessionServiceGrpc.SessionServiceImplBase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.grpc.stub.StreamObserver;
import org.junit.rules.ExternalResource;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Semi-mocked gRPC server that can handle transactions.
 *
 * <p>
 * The {@link #sessionService()} and {@link #requestListener()} are both mock objects and should be used with
 * {@link org.mockito.Mockito#verify(Object)}.
 * </p>
 * <p>
 * By default, the server will return a "done" {@link Transaction.Res} to every message. And will respond
 * with {@link StreamObserver#onCompleted()} when receiving a {@link StreamObserver#onCompleted()} from the client.
 * </p>
 * <p>
 * In order to mock additional responses, use the method {@link #setResponse(Transaction.Req, Transaction.Res...)}.
 * </p>
 *
 */
public final class GraknServerRPCMock extends ExternalResource {

    private final ServerIteratorsMock rpcIterators = new ServerIteratorsMock();

    private final SessionServiceImplBase sessionService;
    private final KeyspaceServiceImplBase keyspaceService;

    @Nullable
    private StreamObserver<Transaction.Res> serverResponses = null;

    @SuppressWarnings("unchecked") // safe because mock
    private StreamObserver<Transaction.Req> requestListener = mock(StreamObserver.class);

    public GraknServerRPCMock(SessionServiceGrpc.SessionServiceImplBase sessionService, KeyspaceServiceGrpc.KeyspaceServiceImplBase keyspaceService) {
        this.sessionService = sessionService;
        this.keyspaceService = keyspaceService;
    }

    SessionServiceImplBase sessionService() {
        return sessionService;
    }


    StreamObserver<Transaction.Req> requestListener() {
        return requestListener;
    }


    void setResponse(Transaction.Req request, Transaction.Res... responses) {
        setResponse(request, Arrays.asList(responses));
    }

    public void setResponse(Transaction.Req request, Throwable throwable) {
        setResponseHandlers(request, ImmutableList.of(TxResponseHandler.onError(throwable)));
    }

    private void setResponse(Transaction.Req request, List<Transaction.Res> responses) {
        setResponseHandlers(request, Lists.transform(responses, TxResponseHandler::onNext));
    }

    private void setResponseHandlers(Transaction.Req request, List<TxResponseHandler> responses) {
        Supplier<TxResponseHandler> next;

        // If there is only one mocked response, just return it again and again
        if (responses.size() > 1) {
            Iterator<TxResponseHandler> iterator = responses.iterator();
            next = iterator::next;
        } else {
            next = () -> Iterables.getOnlyElement(responses);
        }

        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }
            next.get().handle(serverResponses);
            return null;
        }).when(requestListener).onNext(request);
    }

    private interface TxResponseHandler {
        static TxResponseHandler onNext(Transaction.Res response) {
            return streamObserver -> streamObserver.onNext(response);
        }

        static TxResponseHandler onError(Throwable throwable) {
            return streamObserver -> streamObserver.onError(throwable);
        }

        void handle(StreamObserver<Transaction.Res> streamObserver);
    }

    @Override
    protected void before() {
        doAnswer(args -> {
            serverResponses = args.getArgument(0);
            return requestListener;
        }).when(sessionService).transaction(any());

        doAnswer(args -> {
            StreamObserver<KeyspaceProto.Keyspace.Delete.Res> response = args.getArgument(1);
            response.onNext(KeyspaceProto.Keyspace.Delete.Res.newBuilder().build());
            response.onCompleted();
            return null;
        }).when(keyspaceService).delete(any(), any());

        // Return a default "done" response to every message from the client
        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }

            Transaction.Req request = args.getArgument(0);

            Optional<Transaction.Res> next = rpcIterators.next(request.getIterateReq().getId());
            serverResponses.onNext(next.orElse(done()));

            return null;
        }).when(requestListener).onNext(any());

        // Return a default "complete" response to every "complete" message from the client
        doAnswer(args -> {
            if (serverResponses == null) {
                throw new IllegalArgumentException("Set-up of rule not called");
            }
            serverResponses.onCompleted();
            return null;
        }).when(requestListener).onCompleted();
    }

    @Override
    protected void after() {
        if (serverResponses != null) {
            try {
                serverResponses.onCompleted();
            } catch (IllegalStateException e) {
                // this occurs if something has already ended the call
            }
        }
    }

    private static SessionProto.Transaction.Res done() {
        return SessionProto.Transaction.Res.newBuilder()
                .setIterateRes(SessionProto.Transaction.Iter.Res.newBuilder()
                        .setDone(true)).build();
    }

    /**
     * Contains a mutable map of iterators of {@link Transaction.Res}s for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     *
         */
    public class ServerIteratorsMock {
        private final Map<Integer, Iterator<Transaction.Res>> iterators = new ConcurrentHashMap<>();

        /**
         * Return the next response from an iterator. {@link SessionProto.Transaction.Iter.Res} response will return
         * done if the iterator is exhausted.
         */
        public Optional<Transaction.Res> next(int iteratorId) {
            return Optional.ofNullable(iterators.get(iteratorId)).map(iterator -> {
                Transaction.Res response;

                if (iterator.hasNext()) {
                    response = iterator.next();
                } else {
                    response = done();
                    stop(iteratorId);
                }

                return response;
            });
        }

        /**
         * Stop an iterator
         */
        public void stop(int iteratorId) {
            iterators.remove(iteratorId);
        }
    }
}
