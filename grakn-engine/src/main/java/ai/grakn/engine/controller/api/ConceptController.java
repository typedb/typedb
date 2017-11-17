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

package ai.grakn.engine.controller.api;


import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.engine.controller.util.Requests;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknServerException;
import ai.grakn.util.REST.WebPath;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import spark.Request;
import spark.Response;
import spark.Service;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALConceptData;
import static ai.grakn.util.REST.Request.Concept.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Concept.OFFSET_EMBEDDED;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_ALL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static com.codahale.metrics.MetricRegistry.name;

/**
 * <p>
 *     Endpoints used to query for {@link ai.grakn.concept.Concept}s
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class ConceptController {
    private static final int separationDegree = 1;
    private EngineGraknTxFactory factory;
    private Timer conceptIdGetTimer;

    public ConceptController(EngineGraknTxFactory factory, Service spark,
                             MetricRegistry metricRegistry){
        this.factory = factory;
        this.conceptIdGetTimer = metricRegistry.timer(name(ConceptController.class, "concept-by-identifier"));

        spark.get(WebPath.CONCEPT_ID,  this::getConceptById);
    }

    private Json getConceptById(Request request, Response response){
        Requests.validateRequest(request, APPLICATION_ALL, APPLICATION_HAL);

        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE));
        ConceptId conceptId = ConceptId.of(Requests.mandatoryPathParameter(request, ID_PARAMETER));
        int offset = queryParameter(request, OFFSET_EMBEDDED).map(Integer::parseInt).orElse(0);
        int limit = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);
        try(GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = conceptIdGetTimer.time()){
            Concept concept = getConcept(tx, conceptId);

            response.status(HttpStatus.SC_OK);

            return Json.read(renderHALConceptData(concept, false, separationDegree, keyspace, offset, limit));
        }
    }

    private static Concept getConcept(GraknTx tx, ConceptId conceptId){
        Concept concept = tx.getConcept(conceptId);
        if (concept == null) {
            throw GraknServerException.noConceptFound(conceptId, tx.keyspace());
        }
        return concept;
    }
}
