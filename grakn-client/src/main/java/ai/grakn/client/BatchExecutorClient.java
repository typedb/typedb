/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.API;
import ai.grakn.Keyspace;
import ai.grakn.graql.Query;
import ai.grakn.util.SimpleURI;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
import com.netflix.hystrix.HystrixCollapserKey;
import com.netflix.hystrix.HystrixCollapserProperties;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;

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

    private static final Logger LOG = LoggerFactory.getLogger(BatchExecutorClient.class);

    private final GraknClient graknClient;
    private final HystrixRequestContext context;

    // We only allow a certain number of queries to be waiting to execute at once for performance reasons
    private final Semaphore queryExecutionSemaphore;

    // Config
    private final int maxDelay;
    private final int maxRetries;
    private final int maxQueries;
    private final int threadPoolCoreSize;
    private final int timeoutMs;

    // Metrics
    private final MetricRegistry metricRegistry;
    private final Meter failureMeter;
    private final Timer addTimer;
    private final Scheduler scheduler;
    private final ExecutorService executor;
    private boolean requestLogEnabled;

    @Nullable
    private Consumer<? super QueryResponse> queryResponseHandler = null;

    @Nullable
    private Consumer<? super Exception> exceptionHandler = null;

    private final UUID id = UUID.randomUUID();

    private BatchExecutorClient(Builder builder) {
        context = HystrixRequestContext.initializeContext();
        graknClient = builder.graknClient;
        maxDelay = builder.maxDelay;
        maxRetries = builder.maxRetries;
        maxQueries = builder.maxQueries;
        metricRegistry = builder.metricRegistry;
        timeoutMs = builder.timeoutMs;
        threadPoolCoreSize = builder.threadPoolCoreSize;
        requestLogEnabled = builder.requestLogEnabled;
        // Note that the pool on which the observables run is different from the Hystrix pool
        // They need to be of comparable sizes and they should match the capabilities
        // of the server
        executor = Executors.newFixedThreadPool(threadPoolCoreSize);
        scheduler = Schedulers.from(executor);
        queryExecutionSemaphore = new Semaphore(maxQueries);
        addTimer = metricRegistry.timer(name(BatchExecutorClient.class, "add"));
        failureMeter = metricRegistry.meter(name(BatchExecutorClient.class, "failure"));
    }

    /**
     * Will block until there is space for the query to be submitted
     */
    public void add(Query<?> query, Keyspace keyspace) {
        QueryRequest queryRequest = new QueryRequest(query);
        queryRequest.acquirePermit();

        Context contextAddTimer = addTimer.time();
        Observable<QueryResponse> observable = new QueriesObservableCollapser(queryRequest, keyspace)
                .observe()
                .doOnError(error -> failureMeter.mark())
                .subscribeOn(scheduler)
                .doOnTerminate(contextAddTimer::close);

        // We have to subscribe to make the query start loading
        observable.subscribe();
    }

    public void onNext(Consumer<? super QueryResponse> queryResponseHandler) {
        this.queryResponseHandler = queryResponseHandler;
    }

    public void onError(Consumer<? super Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Will block until all submitted queries have executed
     */
    @Override
    public void close() {
        LOG.debug("Closing BatchExecutorClient");

        // Acquire ALL permits. Only possible when all the permits are released.
        // This means this method will only return when ALL the queries are completed.
        LOG.trace("Acquiring all {} permits ({} available)", maxQueries, queryExecutionSemaphore.availablePermits());
        queryExecutionSemaphore.acquireUninterruptibly(maxQueries);
        LOG.trace("Acquired all {} permits ({} available)", maxQueries, queryExecutionSemaphore.availablePermits());

        context.close();
        executor.shutdownNow();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @API
    public static Builder newBuilderforURI(SimpleURI simpleURI) {
        return new Builder().taskClient(GraknClient.of(simpleURI));
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
        private int maxQueries = 10_000;
        private boolean requestLogEnabled = false;
        private MetricRegistry metricRegistry = new MetricRegistry();

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

        public Builder threadPoolCoreSize(int val) {
            threadPoolCoreSize = val;
            return this;
        }

        public Builder metricRegistry(MetricRegistry val) {
            metricRegistry = val;
            return this;
        }

        public Builder maxQueries(int val) {
            maxQueries = val;
            return this;
        }

        public Builder requestLogEnabled(boolean val) {
            requestLogEnabled = val;
            return this;
        }

        public BatchExecutorClient build() {
            return new BatchExecutorClient(this);
        }
    }

    /**
     * Used to make queries with the same text different.
     * We need this because we don't want to cache inserts.
     *
     * This is a non-static class so it can access all fields of {@link BatchExecutorClient}, such as the
     * {@link BatchExecutorClient#queryExecutionSemaphore}. This avoids bugs where Hystrix caches certain parameters
     * or properties like the semaphore: the request is linked directly to the {@link BatchExecutorClient} that
     * created it.
     */
    private class QueryRequest {

        private Query<?> query;
        private UUID id;

        QueryRequest(Query<?> query) {
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
            QueryRequest that = (QueryRequest) o;
            return (query != null ? query.equals(that.query) : that.query == null) && (id != null
                    ? id.equals(that.id) : that.id == null);
        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "QueryRequest{" +
                    "query=" + query +
                    ", id=" + id +
                    '}';
        }

        public Query<?> getQuery() {
            return query;
        }

        void acquirePermit() {
            assert queryExecutionSemaphore.availablePermits() <= maxQueries :
                    "Number of available permits should never exceed max queries";

            // Acquire permission to execute a query - will block until a permit is available
            LOG.trace("Acquiring a permit for {} ({} available)", id, queryExecutionSemaphore.availablePermits());
            queryExecutionSemaphore.acquireUninterruptibly();
            LOG.trace("Acquired a permit for {} ({} available)", id, queryExecutionSemaphore.availablePermits());
        }

        void releasePermit() {
            // Release a query execution permit, allowing a new query to execute
            queryExecutionSemaphore.release();

            int availablePermits = queryExecutionSemaphore.availablePermits();
            LOG.trace("Released a permit for {} ({} available)", id, availablePermits);
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
    private class CommandQueries extends HystrixCommand<List<QueryResponse>> {

        static final int QUEUE_MULTIPLIER = 1024;

        private final List<QueryRequest> queries;
        private final Keyspace keyspace;
        private final Timer graqlExecuteTimer;
        private final Meter attemptMeter;
        private final Retryer<List> retryer;

        CommandQueries(List<QueryRequest> queries, Keyspace keyspace) {
            super(Setter
                    .withGroupKey(HystrixCommandGroupKey.Factory.asKey("BatchExecutor"))
                    .andThreadPoolPropertiesDefaults(
                            HystrixThreadPoolProperties.Setter()
                                    .withCoreSize(threadPoolCoreSize)
                                    // Sizing these two based on the thread pool core size
                                    .withQueueSizeRejectionThreshold(
                                            threadPoolCoreSize * QUEUE_MULTIPLIER)
                                    .withMaxQueueSize(threadPoolCoreSize * QUEUE_MULTIPLIER))
                    .andCommandPropertiesDefaults(
                            HystrixCommandProperties.Setter()
                                    .withExecutionTimeoutEnabled(false)
                                    .withExecutionTimeoutInMilliseconds(timeoutMs)
                                    .withRequestLogEnabled(requestLogEnabled)));

            this.queries = queries;
            this.keyspace = keyspace;
            this.graqlExecuteTimer = metricRegistry.timer(name(this.getClass(), "execute"));
            this.attemptMeter = metricRegistry.meter(name(this.getClass(), "attempt"));
            this.retryer = RetryerBuilder.<List>newBuilder()
                    .retryIfException(throwable ->
                            throwable instanceof GraknClientException
                                    && ((GraknClientException) throwable).isRetriable())
                    .retryIfExceptionOfType(ConnectException.class)
                    .withWaitStrategy(WaitStrategies.exponentialWait(10, 1, TimeUnit.MINUTES))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(maxRetries + 1))
                    .withRetryListener(new RetryListener() {
                        @Override
                        public <V> void onRetry(Attempt<V> attempt) {
                            attemptMeter.mark();
                        }
                    })
                    .build();
        }

        @Override
        protected List run() throws GraknClientException {
            List<Query<?>> queryList = queries.stream().map(QueryRequest::getQuery)
                    .collect(Collectors.toList());
            try {
                List<QueryResponse> responses = retryer.call(() -> {
                    try (Context c = graqlExecuteTimer.time()) {
                        return graknClient.graqlExecute(queryList, keyspace);
                    }
                });

                if (queryResponseHandler != null) {
                    responses.forEach(queryResponseHandler);
                }

                return responses;
            } catch (RetryException | ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof GraknClientException) {
                    if (exceptionHandler != null) {
                        exceptionHandler.accept((GraknClientException) cause);
                    }
                    throw (GraknClientException) cause;
                } else {
                    RuntimeException exception = new RuntimeException("Unexpected exception while retrying, " + queryList.size() + " queries failed.", e);
                    if (exceptionHandler != null) {
                        exceptionHandler.accept(exception);
                    }
                    throw exception;
                }
            } finally {
                queries.forEach(QueryRequest::releasePermit);
            }
        }
    }

    /**
     * This is the hystrix collapser. It's instantiated with a single query but
     * internally it waits until a timeout expires to batch the requests together.
     *
     * @author Domenico Corapi
     */
    private class QueriesObservableCollapser extends HystrixCollapser<List<QueryResponse>, QueryResponse, QueryRequest> {

        private final QueryRequest query;
        private Keyspace keyspace;

        QueriesObservableCollapser(QueryRequest query, Keyspace keyspace) {
            super(Setter
                    .withCollapserKey(hystrixCollapserKey(keyspace))
                    .andCollapserPropertiesDefaults(
                            HystrixCollapserProperties.Setter()
                                    .withRequestCacheEnabled(false)
                                    .withTimerDelayInMilliseconds(maxDelay)
                    )
            );

            this.query = query;
            this.keyspace = keyspace;
        }

        @Override
        public QueryRequest getRequestArgument() {
            return query;
        }

        /**
         * Logic to collapse requests into into CommandQueries
         *
         * @param collapsedRequests Set of requests being collapsed
         * @return returns a command that executed all the requests
         */
        @Override
        protected HystrixCommand<List<QueryResponse>> createCommand(
                Collection<CollapsedRequest<QueryResponse, QueryRequest>> collapsedRequests) {

            List<QueryRequest> requests =
                    collapsedRequests.stream().map(CollapsedRequest::getArgument).collect(Collectors.toList());

            return new CommandQueries(requests, keyspace);
        }

        @Override
        protected void mapResponseToRequests(List<QueryResponse> batchResponse, Collection<CollapsedRequest<QueryResponse, QueryRequest>> collapsedRequests) {
            int count = 0;
            for (CollapsedRequest<QueryResponse, QueryRequest> request : collapsedRequests) {
                QueryResponse response = batchResponse.get(count++);
                request.setResponse(response);
                request.setComplete();
            }
            metricRegistry.histogram(name(QueriesObservableCollapser.class, "batch", "size")).update(collapsedRequests.size());
        }
    }

    private HystrixCollapserKey hystrixCollapserKey(Keyspace keyspace) {
        return HystrixCollapserKey.Factory.asKey(String.format("QueriesObservableCollapser_%s_%s", id, keyspace));
    }
}