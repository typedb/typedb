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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.util.REST;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.engine.controller.GraqlController.getAcceptType;
import static ai.grakn.engine.controller.GraqlController.getMandatoryParameter;
import static ai.grakn.engine.controller.GraqlController.getParameter;
import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.renderHALConceptData;
import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.renderHALConceptOntology;
import static ai.grakn.util.ErrorMessage.NO_CONCEPT_IN_KEYSPACE;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Request.Concept.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Concept.OFFSET_EMBEDDED;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.Graql.IDENTIFIER;

/**
 * <p>
 *     Endpoints used to query the graph by concept type or identifier
 * </p>
 *
 * @author alexandraorth
 */
public class ConceptController {

    private static final int separationDegree = 1;
    private final EngineGraknGraphFactory factory;

    public ConceptController(EngineGraknGraphFactory factory, Service spark){
        this.factory = factory;

        spark.get(REST.WebPath.Concept.CONCEPT,  this::conceptByIdentifier);
    }

    @GET
    @Path("/concept/{id}")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = IDENTIFIER, value = "Identifier of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = KEYSPACE,   value = "Name of graph to use", required = true, dataType = "string", paramType = "query")
    })
    private Json conceptByIdentifier(Request request, Response response){
        validateRequest(request);

        String keyspace = getMandatoryParameter(request, KEYSPACE);
        ConceptId conceptId = ConceptId.of(getMandatoryParameter(request, IDENTIFIER));
        int offset = getParameter(request, OFFSET_EMBEDDED).map(Integer::parseInt).orElse(0);
        int limit = getParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Concept concept = graph.getConcept(conceptId);

            if (notPresent(concept)) {
                throw new GraknEngineServerException(500, NO_CONCEPT_IN_KEYSPACE.getMessage(conceptId, keyspace));
            }

            response.type(APPLICATION_HAL);
            response.status(200);

            if(concept.isType()){
                return Json.read(renderHALConceptOntology(concept, keyspace, offset, limit));
            } else {
                return Json.read(renderHALConceptData(concept, separationDegree, keyspace, offset, limit));
            }
        }
    }

    private void validateRequest(Request request){
        String acceptType = getAcceptType(request);

        if(!acceptType.equals(APPLICATION_HAL)){
            throw new GraknEngineServerException(406, UNSUPPORTED_CONTENT_TYPE, acceptType);
        }
    }

    /**
     * Check if the concept is a valid concept
     * @param concept the concept to validate
     * @return true if the concept is valid, false otherwise
     */
    private boolean notPresent(Concept concept){
        return concept == null;
    }
}
