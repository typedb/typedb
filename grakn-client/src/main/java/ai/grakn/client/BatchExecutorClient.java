/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.client;

import ai.grakn.graql.Query;
import ai.grakn.util.SimpleURI;
import com.netflix.hystrix.HystrixCollapser;
import com.netflix.hystrix.HystrixCollapserProperties;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Client to batch load qraql queries into Grakn that mutate the graph.
 *
 * Provides methods to batch load queries. Optionally can provide a consumer that will execute when
 * a batch finishes loading. BatchExecutorClient will block when the configured resources are being
 * used to execute tasks.
 *
 * @author Domenico Corapi
 */
public class BatchExecutorClient implements Closeable {

    public static final int DEFAULT_TIMEOUT_MS = 60000;
    private final Logger LOG = LoggerFactory.getLogger(BatchExecutorClient.class);

    private final GraknClient graknClient;
    private final HystrixRequestContext context;
    private final int maxDelay;
    private final int maxRetries;

    private BatchExecutorClient(Builder builder) {
        graknClient = builder.graknClient;
        context = HystrixRequestContext.initializeContext();
        maxDelay = builder.maxDelay;
        maxRetries = builder.maxRetries;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilderforURI(SimpleURI simpleURI) {
        return new Builder().taskClient(new GraknClient(simpleURI));
    }

    public Observable<QueryResponse> add(Query<?> query, String keyspace) {
        return new QueriesObservableCollapser(query, keyspace, graknClient, maxDelay)
                .observe()
                .retryWhen(attempts ->
                        attempts.zipWith(Observable.range(1, maxRetries), (num, iter) -> iter)
                                .flatMap(iter -> {
                                    LOG.info("Retrying after " + iter + " second(s)");
                                    return Observable.timer(iter, TimeUnit.SECONDS);
                                }));
    }

    @Override
    public void close() {
        context.shutdown();
    }


    /**
     * Builder
     *
     * @author Domenico Corapi
     */
    public static final class Builder {

        private GraknClient graknClient;
        private int maxDelay = 500;
        private int maxRetries = 5;

        private Builder() {
        }

        public Builder taskClient(GraknClient val) {
            graknClient = val;
            return this;
        }

        public Builder maxDelay(int val) {
            maxDelay = val;
            return this;
        }

        public Builder maxRetries(int val) {
            maxRetries = val;
            return this;
        }

        public BatchExecutorClient build() {
            return new BatchExecutorClient(this);
        }
    }


    /**
     * This is the hystrix command for the batch
     *
     * @author Domenico Corapi
     */
    private static class CommandQueries extends HystrixCommand<List<QueryResponse>> {

        private final List<Query<?>> queries;
        private String keyspace;
        private final GraknClient client;

        public CommandQueries(List<Query<?>> queries, String keyspace, GraknClient client) {
            super(Setter
                    .withGroupKey(HystrixCommandGroupKey.Factory.asKey("BatchMutator"))
                    .andCommandPropertiesDefaults(
                            HystrixCommandProperties.Setter()
                                    .withExecutionTimeoutInMilliseconds(DEFAULT_TIMEOUT_MS)));
            this.queries = queries;
            this.keyspace = keyspace;
            this.client = client;
        }

//        protected Observable<List<QueryResponse>> construct() {
//            CompletableFuture<List<QueryResponse>> future = client.graqlExecute(queries);
//            return Observable.create((Observable.OnSubscribe<List<QueryResponse>>) subscriber -> {
//                future.whenComplete((result, error) -> {
//                    if (error != null) {
//                        subscriber.onError(error);
//                    } else {
//                        subscriber.onNext(result);
//                        subscriber.onCompleted();
//                    }
//                });
//            }).subscribeOn(Schedulers.io());
//        }

        @Override
        protected List<QueryResponse> run() throws Exception {
            return client.graqlExecute(queries, keyspace).get();
        }
    }

    /**
     * This is the hystrix collapser
     *
     * @author Domenico Corapi
     */
    private static class QueriesObservableCollapser extends
            HystrixCollapser<List<QueryResponse>, QueryResponse, Query<?>> {

        private final Query<?> query;
        private String keyspace;
        private final GraknClient client;

        public QueriesObservableCollapser(Query<?> query, String keyspace,
                GraknClient client, int delay) {
            super(Setter.withCollapserKey(com.netflix.hystrix.HystrixCollapserKey.Factory.asKey("BatchMutatorCollapser"))
                    .andCollapserPropertiesDefaults(
                            HystrixCollapserProperties.Setter()
                                    .withTimerDelayInMilliseconds(delay)));

            this.query = query;
            this.keyspace = keyspace;
            this.client = client;
        }

        @Override
        public Query<?> getRequestArgument() {
            return query;
        }

        @Override
        protected HystrixCommand<List<QueryResponse>> createCommand(
                Collection<CollapsedRequest<QueryResponse, Query<?>>> collapsedRequests) {
            return new CommandQueries(collapsedRequests.stream().map(CollapsedRequest::getArgument)
                    .collect(Collectors.toList()), keyspace, client);
        }

        @Override
        protected void mapResponseToRequests(List<QueryResponse> batchResponse,
                Collection<CollapsedRequest<QueryResponse, Query<?>>> collapsedRequests) {
            int count = 0;
            for (CollapsedRequest<QueryResponse, Query<?>> request : collapsedRequests) {
                request.setResponse(batchResponse.get(count++));
            }
        }
    }


}