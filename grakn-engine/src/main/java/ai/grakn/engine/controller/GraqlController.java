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

package ai.grakn.engine.controller;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.controller.response.ExplanationBuilder;
import ai.grakn.engine.controller.util.Requests;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.attribute.uniqueness.AttributeUniqueness;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.internal.printer.Printer;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import mjson.Json;
import org.apache.commons.lang.StringUtils;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.util.REST.Request.Graql.ALLOW_MULTIPLE_QUERIES;
import static ai.grakn.util.REST.Request.Graql.DEFINE_ALL_VARS;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.LOADING_DATA;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.Graql.TX_TYPE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.Boolean.parseBoolean;
import static org.apache.http.HttpStatus.SC_OK;


/**
 * Endpoints used to query the graph using Graql and build a HAL, Graql or Json response.
 *
 * @author Marco Scoppetta
 */
public class GraqlController implements HttpController {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(GraqlController.class);
    private static final RetryLogger retryLogger = new RetryLogger();
    private static final int MAX_RETRY = 10;
    private final Printer printer;
    private final EngineGraknTxFactory factory;
    private final PostProcessor postProcessor;
    private final Timer executeGraql;
    private final Timer executeExplanation;

    public GraqlController(
            EngineGraknTxFactory factory, PostProcessor postProcessor, Printer printer, MetricRegistry metricRegistry
    ) {
        this.factory = factory;
        this.postProcessor = postProcessor;
        this.printer = printer;
        this.executeGraql = metricRegistry.timer(name(GraqlController.class, "execute-graql"));
        this.executeExplanation = metricRegistry.timer(name(GraqlController.class, "execute-explanation"));
    }

    @Override
    public void start(Service spark) {
        spark.post(REST.WebPath.KEYSPACE_GRAQL, this::executeGraql);
        spark.get(REST.WebPath.KEYSPACE_EXPLAIN, this::explainGraql);

        spark.exception(GraqlQueryException.class, (e, req, res) -> handleError(400, e, res));
        spark.exception(GraqlSyntaxException.class, (e, req, res) -> handleError(400, e, res));

        // Handle invalid type castings and invalid insertions
        spark.exception(GraknTxOperationException.class, (e, req, res) -> handleError(422, e, res));
        spark.exception(InvalidKBException.class, (e, req, res) -> handleError(422, e, res));
    }

    @GET
    @Path("/kb/{keyspace}/explain")
    private String explainGraql(Request request, Response response) throws RetryException, ExecutionException {
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        String queryString = mandatoryQueryParameter(request, QUERY);

        response.status(SC_OK);

        return executeFunctionWithRetrying(() -> {
            try (GraknTx tx = factory.tx(keyspace, GraknTxType.WRITE); Timer.Context context = executeExplanation.time()) {
                ConceptMap answer = tx.graql().infer(true).parser().<GetQuery>parseQuery(queryString).execute().stream().findFirst().orElse(new ConceptMapImpl());
                return mapper.writeValueAsString(ExplanationBuilder.buildExplanation(answer));
            }
        });
    }

    @POST
    @Path("/kb/{keyspace}/graql")
    private String executeGraql(Request request, Response response) throws RetryException, ExecutionException {
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        String queryString = mandatoryBody(request);

        //Run the query with reasoning on or off
        Boolean infer = parseBoolean(queryParameter(request, EXECUTE_WITH_INFERENCE));

        //Allow multiple queries to be executed
        boolean multiQuery = parseBoolean(queryParameter(request, ALLOW_MULTIPLE_QUERIES));

        //Define all anonymous variables in the query
        Boolean defineAllVars = parseBoolean(queryParameter(request, DEFINE_ALL_VARS));

        //Used to check if serialisation of results is needed. When loading we skip this for the sake of speed
        boolean skipSerialisation = parseBoolean(queryParameter(request, LOADING_DATA));

        //Check the transaction type to use
        String txStr = queryParameter(request, TX_TYPE);
        GraknTxType txType = txStr != null ? GraknTxType.valueOf(txStr.toUpperCase(Locale.getDefault())) : GraknTxType.WRITE;

        //This is used to determine the response format
        //TODO: Maybe we should really try to stick with one representation? This would require dashboard console interpreting the json representation
        final String acceptType;
        if (APPLICATION_TEXT.equals(Requests.getAcceptType(request))) {
            acceptType = APPLICATION_TEXT;
        } else {
            acceptType = APPLICATION_JSON;
        }
        response.type(APPLICATION_JSON);

        //Execute the query and get the results
        LOG.debug("Executing graql query: {}", StringUtils.abbreviate(queryString, 100));
        LOG.trace("Full query: {}", queryString);

        return executeFunctionWithRetrying(() -> {
            try (EmbeddedGraknTx<?> tx = factory.tx(keyspace, txType); Timer.Context context = executeGraql.time()) {

                QueryBuilder builder = tx.graql();
                if (infer != null) builder.infer(infer);

                QueryParser parser = builder.parser();
                if (defineAllVars != null) parser.defineAllVars(defineAllVars);

                response.status(SC_OK);

                return executeQuery(tx, queryString, acceptType, multiQuery, skipSerialisation, parser);
            } finally {
                LOG.debug("Executed graql query");
            }
        });
    }

    private String executeFunctionWithRetrying(Callable<String> callable) throws RetryException, ExecutionException {
        try {
            Retryer<String> retryer = RetryerBuilder.<String>newBuilder()
                    .retryIfExceptionOfType(TemporaryWriteException.class)
                    .withRetryListener(retryLogger)
                    .withWaitStrategy(WaitStrategies.exponentialWait(100, 5, TimeUnit.MINUTES))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_RETRY))
                    .build();

            return retryer.call(callable);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw e;
            }
        }
    }

    private static class RetryLogger implements RetryListener {
        @Override
        public <V> void onRetry(Attempt<V> attempt) {
            if (attempt.hasException()) {
                LOG.warn("Retrying transaction after {" + attempt.getAttemptNumber() + "} attempts due to exception {" + attempt.getExceptionCause().getMessage() + "}");
            }
        }
    }


    /**
     * Handle any {@link Exception} that are thrown by the server. Configures and returns
     * the correct JSON response with the given status.
     *
     * @param exception exception thrown by the server
     * @param response  response to the client
     */
    private static void handleError(int status, Exception exception, Response response) {
        LOG.error("REST error", exception);
        response.status(status);
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }

//    /**
//     * Execute a query and return a response in the format specified by the request.
//     *
//     * @param tx          open transaction to current graph
//     * @param queryString read query to be executed
//     * @param acceptType  response format that the client will accept
//     * @param multi       execute multiple statements
//     * @param parser
//     */
//    private String executeQuery(EmbeddedGraknTx<?> tx, String queryString, String acceptType, boolean multi, boolean skipSerialisation, QueryParser parser) throws JsonProcessingException {
//
//        // By default use Jackson printer
//        Printer<?> printer = this.printer;
//
//        if (APPLICATION_TEXT.equals(acceptType)) printer = Printer.stringPrinter(false);
//
//        String formatted;
//        boolean commitQuery = true;
//        if (multi) {
//            Stream<Query<?>> query = parser.parseList(queryString);
//            List<?> collectedResults = query.map(this::executeAndMonitor).collect(Collectors.toList());
//            if (skipSerialisation) {
//                formatted = mapper.writeValueAsString(new Object[collectedResults.size()]);
//            } else {
//                formatted = printer.toString(collectedResults);
//            }
//        } else {
//            Query<?> query = parser.parseQuery(queryString);
//            if (skipSerialisation) {
//                formatted = "";
//            } else {
//                // If acceptType is 'application/text' add new line after every result
//                if (APPLICATION_TEXT.equals(acceptType)) {
//                    //TODO: remove this if check once all queries becomes streamable (nb: have stream() not implement Streamable<>)
//                    if (query instanceof Streamable) {
//                        formatted = printer.toStream(((Streamable<?>) query).stream()).collect(Collectors.joining("\n"));
//                    } else {
//                        formatted = printer.toString(query.execute());
//                    }
//                } else {
//                    // If acceptType is 'application/json' map results to JSON representation
//                    formatted = printer.toString(executeAndMonitor(query));
//                }
//
//            }
//            commitQuery = !query.isReadOnly();
//        }
//
//        if (commitQuery) tx.commitSubmitNoLogs().ifPresent(postProcessor::submit);
//
//        return formatted;
//    }

    /**
     * Execute a query and return a response in the format specified by the request.
     *
     * @param tx          open transaction to current graph
     * @param queryString read query to be executed
     * @param acceptType  response format that the client will accept
     * @param multi       execute multiple statements
     * @param parser
     */
    private String executeQuery(EmbeddedGraknTx<?> tx, String queryString, String acceptType, boolean multi, boolean skipSerialisation, QueryParser parser) throws JsonProcessingException {

        // By default use Jackson printer
        Printer<?> printer = this.printer;

        if (APPLICATION_TEXT.equals(acceptType)) printer = Printer.stringPrinter(false);

        String formatted;
        boolean commitQuery = true;
        if (multi) {
            Stream<Query<?>> query = parser.parseList(queryString);
            List<?> collectedResults = query.map(this::executeAndMonitor).collect(Collectors.toList());
            if (skipSerialisation) {
                formatted = mapper.writeValueAsString(new Object[collectedResults.size()]);
            } else {
                formatted = printer.toString(collectedResults);
            }
        } else {
            Query<?> query = parser.parseQuery(queryString);
            if (skipSerialisation) {
                formatted = "";
            } else {
                // If acceptType is 'application/text' add new line after every result
                if (APPLICATION_TEXT.equals(acceptType)) {
                    formatted = printer.toStream(query.stream()).collect(Collectors.joining("\n"));
                } else {
                    // If acceptType is 'application/json' map results to JSON representation
                    formatted = printer.toString(executeAndMonitor(query));
                }

            }
            commitQuery = !query.isReadOnly();
        }

        if (commitQuery) {
            tx.commitAndGetLogs().ifPresent(commitLog ->
                    commitLog.attributes().forEach((value, conceptIds) ->
                            conceptIds.forEach(id -> AttributeUniqueness.singleton.insertAttribute(commitLog.keyspace(), value, id))
                    )
            );
        }

        return formatted;
    }
    private Object executeAndMonitor(Query<?> query) {
        return query.execute();
    }

}
