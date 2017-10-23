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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
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

    private final static Logger LOG = LoggerFactory.getLogger(BatchExecutorClient.class);

    private final GraknClient graknClient;
    private final HystrixRequestContext context;

    // Config
    private final int maxDelay;
    private final int maxRetries;
    private final int threadPoolCoreSize;
    private final int timeoutMs;

    // Metrics
    private final MetricRegistry metricRegistry;
    private final Meter failureMeter;
    private final Timer addTimer;

    private BatchExecutorClient(Builder builder) {
        context = HystrixRequestContext.initializeContext();
        graknClient = builder.graknClient;
        maxDelay = builder.maxDelay;
        maxRetries = builder.maxRetries;
        metricRegistry = builder.metricRegistry;
        timeoutMs = builder.timeoutMs;
        threadPoolCoreSize = builder.threadPoolCoreSize;
        addTimer = metricRegistry.timer(name(BatchExecutorClient.class, "add"));
        failureMeter = metricRegistry.meter(name(BatchExecutorClient.class, "failure"));
    }

    public Observable<QueryResponse> add(Query<?> query, String keyspace) {
        return add(query, keyspace, true);
    }

    public Observable<QueryResponse> add(Query<?> query, String keyspace, boolean keepErrors) {
        Context context = addTimer.time();
        Observable<QueryResponse> observable = new QueriesObservableCollapser(query,
                keyspace, graknClient, maxDelay, maxRetries, threadPoolCoreSize, timeoutMs, metricRegistry)
                .observe()
                .doOnError((error) -> failureMeter.mark())
                .doOnEach(a -> LOG.debug("Executed {}", a.getValue()))
                .doOnTerminate(context::close);
        return keepErrors ? observable : ignoreErrors(observable);
    }

    private Observable<QueryResponse> ignoreErrors(Observable<QueryResponse> observable) {
        observable = observable
                .map(Optional::of)
                .onErrorResumeNext(error -> {
                    LOG.error("Error while executing query but skipping: {}", error.getMessage());
                    return Observable.just(Optional.empty());
                }).filter(Optional::isPresent).map(Optional::get);
        return observable;
    }

    @Override
    public void close() {
        context.close();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilderforURI(String simpleURI) {
        return new Builder().taskClient(new GraknClient(new SimpleURI(simpleURI)));
    }

    /**
     * Builder
     *
     * @author Domenico Corapi
     */
    public static final class Builder {
        private GraknClient graknClient;
        private int maxDelay = 50;
        private int maxRetries = 5;
        private int threadPoolCoreSize = 8;
        private int timeoutMs = 60_000;
        private MetricRegistry metricRegistry = new MetricRegistry();

        private Builder() {}

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

        public Builder threadPoolCoreSize(int val) {
            threadPoolCoreSize = val;
            return this;
        }

        public Builder metricRegistry(MetricRegistry val) {
            metricRegistry = val;
            return this;
        }

        public Builder metricRegistry(int val) {
            timeoutMs = val;
            return this;
        }

        public BatchExecutorClient build() {
            return new BatchExecutorClient(this);
        }
    }

    // Used to make queries with the same text different
    // We need this because we don't want to cache inserts
    private static class QueryWithId<T> {
        private Query<T> query;
        private UUID id;

        public QueryWithId(Query<T> query) {
            this.query = query;
            this.id = UUID.randomUUID();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QueryWithId<?> that = (QueryWithId<?>) o;
            return (query != null ? query.equals(that.query) : that.query == null) && (id != null
                    ? id.equals(that.id) : that.id == null);
        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

        public Query<T> getQuery() {
            return query;
        }
    }

    // Internal commands

    /*
     * The Batch Executor client uses Hystrix to batch requests. As a positive side effect
     * we get the Hystrix circuit breaker too.
     * Hystrix wraps every thing that it does inside a Command. A Command defines what happens
     * when it's run, and optionally a fallback. Here in CommandQueries, we just define the run.
     * The batching is implemented using a Collapser, in our case
     * it's the QueriesObservableCollapser.
     * See the classes Javadocs for more info.
     */


    /**
     * This is the hystrix command for the batch. If collapsing weren't performed
     * we would call this command directly passing a set of queries.
     * Within the collapsing logic, this command is called after a certain timeout
     * expires to batch requests together.
     *
     * @author Domenico Corapi
     */
    private static class CommandQueries extends HystrixCommand<List<QueryResponse>> {

        static final int QUEUE_MULTIPLIER = 16;

        private final List<QueryWithId<?>> queries;
        private final String keyspace;
        private final GraknClient client;
        private final Timer graqlExecuteTimer;
        private final Meter attemptMeter;
        private final Retryer<List<QueryResponse>> retryer;

        CommandQueries(List<QueryWithId<?>> queries, String keyspace, GraknClient client,
                int retries, int threadPoolCoreSize, int timeoutMs,
                MetricRegistry metricRegistry) {
            super(Setter
                    .withGroupKey(HystrixCommandGroupKey.Factory.asKey("BatchExecutor"))
                    .andThreadPoolPropertiesDefaults(
                            HystrixThreadPoolProperties.Setter()
                                    .withCoreSize(threadPoolCoreSize)
                                    // Sizing these two based on the thread pool core size
                                    .withQueueSizeRejectionThreshold(threadPoolCoreSize * QUEUE_MULTIPLIER)
                                    .withMaxQueueSize(threadPoolCoreSize * QUEUE_MULTIPLIER))
                    .andCommandPropertiesDefaults(
                            HystrixCommandProperties.Setter()
                                    .withExecutionTimeoutEnabled(false)
                                    .withExecutionTimeoutInMilliseconds(timeoutMs)));
            this.queries = queries;
            this.keyspace = keyspace;
            this.client = client;
            this.graqlExecuteTimer = metricRegistry.timer(name(this.getClass(), "execute"));
            this.attemptMeter = metricRegistry.meter(name(this.getClass(), "attempt"));
            this.retryer = RetryerBuilder.<List<QueryResponse>>newBuilder()
                    .retryIfException((throwable) ->
                            throwable instanceof GraknClientException
                                    && ((GraknClientException) throwable).isRetriable())
                    .withWaitStrategy(WaitStrategies.exponentialWait(10, 1, TimeUnit.MINUTES))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(retries + 1))
                    .withRetryListener(new RetryListener() {
                        @Override
                        public <V> void onRetry(Attempt<V> attempt) {
                            attemptMeter.mark();
                        }
                    })
                    .build();
        }

        @Override
        protected List<QueryResponse> run() throws GraknClientException {
            LOG.debug("Running queries on keyspace {}: {}", keyspace, queries);
            try {
                return retryer.call(() -> {
                    try (Context c = graqlExecuteTimer.time()) {
                        return client.graqlExecute(queries.stream().map(QueryWithId::getQuery)
                                .collect(Collectors.toList()), keyspace);
                    }
                });
            } catch (RetryException | ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof GraknClientException) {
                    throw (GraknClientException) cause;
                } else {
                    throw new RuntimeException("Unexpected exception while retrying", e);
                }
            }
        }
    }

    /**
     * This is the hystrix collapser. It's instantiated with a single query but
     * internally it waits until a timeout expires to batch the requests together.
     *
     * @author Domenico Corapi
     */
    private static class QueriesObservableCollapser extends
            HystrixCollapser<List<QueryResponse>, QueryResponse, QueryWithId<?>> {

        private final QueryWithId<?> query;
        private String keyspace;
        private final GraknClient client;
        private final int retries;
        private int threadPoolCoreSize;
        private int timeoutMs;
        private final MetricRegistry metricRegistry;

        public QueriesObservableCollapser(Query<?> query, String keyspace,
                GraknClient client, int delay, int retries, int threadPoolCoreSize, int timeoutMs, MetricRegistry metricRegistry) {
            super(Setter.withCollapserKey(
                    // It split by keyspace since we want to avoid mixing requests for different
                    // keyspaces together
                    com.netflix.hystrix.HystrixCollapserKey.Factory
                            .asKey("QueriesObservableCollapser_" + keyspace))
                    .andCollapserPropertiesDefaults(
                            HystrixCollapserProperties.Setter()
                                    .withRequestCacheEnabled(false)
                                    .withTimerDelayInMilliseconds(delay)));
            this.query = new QueryWithId<>(query);
            this.keyspace = keyspace;
            this.client = client;
            this.retries = retries;
            this.threadPoolCoreSize = threadPoolCoreSize;
            this.timeoutMs = timeoutMs;
            this.metricRegistry = metricRegistry;
        }

        @Override
        public QueryWithId<?> getRequestArgument() {
            return query;
        }

        /**
         * Logic to collapse requests into into CommandQueries
         * @param   collapsedRequests Set of requests being collapsed
         * @return  returns a command that executed all the requests
         */
        @Override
        protected HystrixCommand<List<QueryResponse>> createCommand(
                Collection<CollapsedRequest<QueryResponse, QueryWithId<?>>> collapsedRequests) {
            return new CommandQueries(collapsedRequests.stream().map(CollapsedRequest::getArgument)
                    .collect(Collectors.toList()), keyspace, client, retries, threadPoolCoreSize, timeoutMs, metricRegistry);
        }

        @Override
        protected void mapResponseToRequests(List<QueryResponse> batchResponse,
                Collection<CollapsedRequest<QueryResponse, QueryWithId<?>>> collapsedRequests) {
            int count = 0;
            for (CollapsedRequest<QueryResponse, QueryWithId<?>> request : collapsedRequests) {
                request.setResponse(batchResponse.get(count++));
            }
            metricRegistry.histogram(name(QueriesObservableCollapser.class, "batch", "size"))
                    .update(count);
        }
    }


}