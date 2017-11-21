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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.engine.controller.util.Requests;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.util.REST.WebPath;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.LABEL_PARAMETER;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_ALL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;

/**
 * <p>
 *     Endpoints used to query for {@link ai.grakn.concept.Concept}s
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class ConceptController {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private EngineGraknTxFactory factory;
    private Timer conceptIdGetTimer;
    private Timer labelGetTimer;

    public ConceptController(EngineGraknTxFactory factory, Service spark,
                             MetricRegistry metricRegistry){
        this.factory = factory;
        this.conceptIdGetTimer = metricRegistry.timer(name(ConceptController.class, "concept-by-identifier"));
        this.labelGetTimer = metricRegistry.timer(name(ConceptController.class, "concept-by-label"));

        spark.get(WebPath.CONCEPT_ID,  this::getConceptById);
        spark.get(WebPath.TYPE_LABEL,  this::getSchemaByLabel);
        spark.get(WebPath.RULE_LABEL,  this::getSchemaByLabel);
        spark.get(WebPath.ROLE_LABEL,  this::getSchemaByLabel);

        spark.get(WebPath.KEYSPACE_TYPE, this::getTypes);
        spark.get(WebPath.KEYSPACE_RULE, this::getRules);
        spark.get(WebPath.KEYSPACE_ROLE, this::getRoles);
    }

    private String getSchemaByLabel(Request request, Response response) throws JsonProcessingException {
        Requests.validateRequest(request, APPLICATION_ALL, APPLICATION_HAL);
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        Label label = Label.of(mandatoryPathParameter(request, LABEL_PARAMETER));
        return getConcept(response, keyspace, (tx) -> tx.getSchemaConcept(label));
    }

    private String getConceptById(Request request, Response response) throws JsonProcessingException {
        Requests.validateRequest(request, APPLICATION_ALL, APPLICATION_HAL);
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        ConceptId conceptId = ConceptId.of(mandatoryPathParameter(request, ID_PARAMETER));
        return getConcept(response, keyspace, (tx) -> tx.getConcept(conceptId));
    }

    private String getConcept(Response response, Keyspace keyspace, Function<GraknTx, ai.grakn.concept.Concept> getter) throws JsonProcessingException {
        response.type(APPLICATION_JSON);

        try (GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = conceptIdGetTimer.time()) {
            ai.grakn.concept.Concept concept = getter.apply(tx);

            Optional<Concept> conceptWrapper = Optional.ofNullable(concept).map(ConceptBuilder::build);
            if(conceptWrapper.isPresent()){
                response.status(SC_OK);
                return objectMapper.writeValueAsString(conceptWrapper.get());
            } else {
                response.status(SC_NOT_FOUND);
                return "";
            }
        }
    }

    private String getTypes(Request request, Response response) throws JsonProcessingException {
        Function<GraknTx, Stream<? extends ai.grakn.concept.Concept>> getter = (tx) -> {
            Stream<? extends ai.grakn.concept.Concept> stream = tx.admin().getMetaEntityType().subs();
            stream = Stream.concat(stream, tx.admin().getMetaRelationType().subs());
            stream = Stream.concat(stream, tx.admin().getMetaAttributeType().subs());
            return stream;
        };

        return getConcepts(request, response, getter);
    }

    private String getRules(Request request, Response response) throws JsonProcessingException {
        return getConcepts(request, response, (tx) -> tx.admin().getMetaRule().subs());
    }

    private String getRoles(Request request, Response response) throws JsonProcessingException {
        return getConcepts(request, response, (tx) -> tx.admin().getMetaRole().subs());
    }

    private String getConcepts(Request request, Response response, Function<GraknTx, Stream<? extends ai.grakn.concept.Concept>>getter) throws JsonProcessingException {
        response.type(APPLICATION_JSON);

        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));

        try (GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = labelGetTimer.time()) {
            Set<Concept> concepts = getter.apply(tx).map(ConceptBuilder::<Concept>build).collect(Collectors.toSet());
            return objectMapper.writeValueAsString(concepts);
        }
    }
}
