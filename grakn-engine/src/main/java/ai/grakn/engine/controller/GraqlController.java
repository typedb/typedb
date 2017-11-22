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

package ai.grakn.engine.controller;


import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.QueryParser;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.GraknTxType.WRITE;
import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.util.REST.Request.Graql.ALLOW_MULTIPLE_QUERIES;
import static ai.grakn.util.REST.Request.Graql.DEFINE_ALL_VARS;
import static ai.grakn.util.REST.Request.Graql.EXECUTE_WITH_INFERENCE;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.Boolean.parseBoolean;

/**
 * <p>
 *     Endpoints used to query for {@link ai.grakn.concept.Concept}s via the graql language
 * </p>
 *
 * @author Marco Scoppetta, alexandraorth, Filipe Peliz Pinto Teixeira
 */
public class GraqlController {
    private static final Logger LOG = LoggerFactory.getLogger(DeprecatedGraqlController.class);
    private final EngineGraknTxFactory factory;
    private final Timer executeGraql;

    public GraqlController(EngineGraknTxFactory factory, Service spark, MetricRegistry metricRegistry) {
        this.factory = factory;
        this.executeGraql = metricRegistry.timer(name(DeprecatedGraqlController.class, "execute-graql"));

        spark.post(REST.WebPath.KEYSPACE_GRAQL, this::executeGraql);

        spark.exception(GraqlQueryException.class, (e, req, res) -> handleError(400, e, res));
        spark.exception(GraqlSyntaxException.class, (e, req, res) -> handleError(400, e, res));

        // Handle invalid type castings and invalid insertions
        spark.exception(GraknTxOperationException.class, (e, req, res) -> handleError(422, e, res));
        spark.exception(InvalidKBException.class, (e, req, res) -> handleError(422, e, res));
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

    private String executeGraql(Request request, Response response) {
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        String queryString = mandatoryBody(request);

        //Limits the number of results returned
        int limitEmbedded = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

        //Run the query with reasoning on or off
        Optional<Boolean> infer = queryParameter(request, EXECUTE_WITH_INFERENCE).map(Boolean::parseBoolean);

        //Allow multiple queries to be executed
        boolean multiQuery = parseBoolean(queryParameter(request, ALLOW_MULTIPLE_QUERIES).orElse("false"));

        //Define all anonymous variables in the query
        Optional<Boolean> defineAllVars = queryParameter(request, DEFINE_ALL_VARS).map(Boolean::parseBoolean);

        //Execute the query and get the results
        try (GraknTx graph = factory.tx(keyspace, WRITE); Timer.Context context = executeGraql.time()) {

            //Configure how the query should be executed
            QueryBuilder builder = graph.graql();
            infer.ifPresent(builder::infer);

            QueryParser parser = builder.parser();
            defineAllVars.ifPresent(parser::defineAllVars);

            if(multiQuery){
                Stream<Query<?>> query = parser.parseList(queryString);
                List<?> collectedResults = query.map(Query::execute).collect(Collectors.toList());
            }


        }

        return "";
    }


}
