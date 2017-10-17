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

import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.netflix.hystrix.HystrixCollapser;
import com.netflix.hystrix.HystrixCollapserProperties;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    private final static Logger LOG = LoggerFactory.getLogger(BatchExecutorClient.class);

    static final int DEFAULT_TIMEOUT_MS = 60000;

    private final GraknClient graknClient;
    private final HystrixRequestContext context;
    private final int maxDelay;
//    private final int maxRetries;
    private final MetricRegistry metricRegistry;
    private final Timer addTimer;
    private final Meter failureMeter;
//    private final Meter retryMeter;

    private BatchExecutorClient(Builder builder) {
        graknClient = builder.graknClient;
        context = HystrixRequestContext.initializeContext();
        maxDelay = builder.maxDelay;
//        maxRetries = builder.maxRetries;
        metricRegistry = new MetricRegistry();
        addTimer = metricRegistry
                .timer(name(BatchExecutorClient.class, "add"));
        failureMeter = metricRegistry
                .meter(name(BatchExecutorClient.class, "failure"));
//        retryMeter = metricRegistry
//                .meter(name(BatchExecutorClient.class, "retry"));
        if (builder.reportStats) {
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(MILLISECONDS)
                    .build();
            reporter.start(1, TimeUnit.MINUTES);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilderforURI(SimpleURI simpleURI) {
        return new Builder().taskClient(new GraknClient(simpleURI));
    }

    public Observable<QueryResponse> add(Query<?> query, String keyspace) {
        Context context = addTimer.time();
        return new QueriesObservableCollapser(query, keyspace, graknClient, maxDelay, metricRegistry)
                .observe()
//                .retryWhen(errors->
//                        errors
//                        .zipWith(Observable.range(1,maxRetries),(err,attempt)-> {
//                            retryMeter.mark();
//                            long s = (long) Math.pow(4, attempt);
//                            LOG.info("Retrying after " + s + " second(s)");
//                                return attempt < maxRetries?
//                                        Observable.timer(s,TimeUnit.SECONDS):
//                                        Observable.error(err);})
//                        .flatMap(x-> x)
//                )
                .doOnError((error) -> {
                    failureMeter.mark();
                })
                .doOnTerminate(context::close);
    }

    public boolean keyspaceExists(Keyspace keyspace) {
        try {
            return graknClient.keyspace(keyspace.getValue()).equals(keyspace);
        } catch (GraknClientException e) {
            return false;
        }
    }

    @Override
    public void close() {
        context.close();
    }



    /**
     * Builder
     *
     * @author Domenico Corapi
     */
    public static final class Builder {

        private GraknClient graknClient;
        private int maxDelay = 500;
//        private int maxRetries = 5;
        private boolean reportStats = true;

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
//            maxRetries = val;
            return this;
        }

        public void reportStats(boolean val) {
            this.reportStats = val;
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
                    .withGroupKey(HystrixCommandGroupKey.Factory.asKey("BatchExecutor"))
                    .andThreadPoolPropertiesDefaults(
                            HystrixThreadPoolProperties.Setter()
                                    .withQueueSizeRejectionThreshold(500)
                                    .withMaxQueueSize(500))
                    .andCommandPropertiesDefaults(
                            HystrixCommandProperties.Setter()
                                    .withExecutionTimeoutEnabled(false)
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
            LOG.info("Running on keyspace {}: {}",  keyspace, queries);
            return client.graqlExecute(queries, keyspace);
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
        private final MetricRegistry metricRegistry;

        public QueriesObservableCollapser(Query<?> query, String keyspace,
                GraknClient client, int delay, MetricRegistry metricRegistry) {
            super(Setter.withCollapserKey(
                    com.netflix.hystrix.HystrixCollapserKey.Factory.asKey("QueriesObservableCollapser_" + keyspace))
                    .andCollapserPropertiesDefaults(
                            HystrixCollapserProperties.Setter()
                                    .withRequestCacheEnabled(false)
                                    .withTimerDelayInMilliseconds(delay)));
            this.query = query;
            this.keyspace = keyspace;
            this.client = client;
            this.metricRegistry = metricRegistry;
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
            metricRegistry.histogram(name(QueriesObservableCollapser.class, "batch", "size"))
                    .update(count);
        }
    }


}