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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller;


import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.engine.Jacksonisable;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.engine.controller.response.EmbeddedAttribute;
import ai.grakn.engine.controller.response.Link;
import ai.grakn.engine.controller.response.ListResource;
import ai.grakn.engine.controller.response.RolePlayer;
import ai.grakn.engine.controller.response.Things;
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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.LIMIT_PARAMETER;
import static ai.grakn.util.REST.Request.OFFSET_PARAMETER;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_ALL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;

/**
 * Endpoints used to query for {@link ai.grakn.concept.Concept}s
 *
 * @author Grakn Warriors
 */
public class ConceptController implements HttpController {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private EngineGraknTxFactory factory;
    private Timer conceptIdGetTimer;
    private Timer labelGetTimer;
    private Timer instancesGetTimer;

    public ConceptController(EngineGraknTxFactory factory,
                             MetricRegistry metricRegistry){
        this.factory = factory;
        this.conceptIdGetTimer = metricRegistry.timer(name(ConceptController.class, "concept-by-identifier"));
        this.labelGetTimer = metricRegistry.timer(name(ConceptController.class, "concept-by-label"));
        this.instancesGetTimer = metricRegistry.timer(name(ConceptController.class, "instances-of-type"));
    }

    @Override
    public void start(Service spark) {
        spark.get(WebPath.CONCEPT_ID,  this::getConceptById);
        spark.get(WebPath.TYPE_LABEL,  this::getSchemaByLabel);
        spark.get(WebPath.RULE_LABEL,  this::getSchemaByLabel);
        spark.get(WebPath.ROLE_LABEL,  this::getSchemaByLabel);

        spark.get(WebPath.KEYSPACE_TYPE, this::getTypes);
        spark.get(WebPath.KEYSPACE_RULE, this::getRules);
        spark.get(WebPath.KEYSPACE_ROLE, this::getRoles);

        spark.get(WebPath.CONCEPT_ATTRIBUTES, this::getAttributes);
        spark.get(WebPath.CONCEPT_KEYS, this::getKeys);
        spark.get(WebPath.CONCEPT_RELATIONSHIPS, this::getRelationships);

        spark.get(WebPath.TYPE_INSTANCES, this::getTypeInstances);
        spark.get(WebPath.TYPE_PLAYS, this::getTypePlays);
        spark.get(WebPath.TYPE_ATTRIBUTES, this::getTypeAttributes);
        spark.get(WebPath.TYPE_KEYS, this::getTypeKeys);

        spark.get(WebPath.TYPE_SUBS, this::getSchemaConceptSubs);
        spark.get(WebPath.ROLE_SUBS, this::getSchemaConceptSubs);
        spark.get(WebPath.RULE_SUBS, this::getSchemaConceptSubs);
    }

    private String getTypeAttributes(Request request, Response response) throws JsonProcessingException {
        Function<ai.grakn.concept.Type, Stream<Jacksonisable>> collector = type -> type.attributes().map(ConceptBuilder::build);
        return getConceptCollection(request, response, "attributes", buildTypeGetter(request), collector);
    }

    private String getTypeKeys(Request request, Response response) throws JsonProcessingException {
        Function<ai.grakn.concept.Type, Stream<Jacksonisable>> collector = type -> type.keys().map(ConceptBuilder::build);
        return getConceptCollection(request, response, "keys", buildTypeGetter(request), collector);
    }

    private String getTypePlays(Request request, Response response) throws JsonProcessingException {
        Function<ai.grakn.concept.Type, Stream<Jacksonisable>> collector = type -> type.playing().map(ConceptBuilder::build);
        return getConceptCollection(request, response, "plays", buildTypeGetter(request), collector);
    }

    private String getRelationships(Request request, Response response) throws JsonProcessingException {
        //TODO: Figure out how to incorporate offset and limit
        Function<ai.grakn.concept.Thing, Stream<Jacksonisable>> collector = thing -> thing.roles().flatMap(role -> {
            Link roleWrapper = Link.create(role);
            return thing.relationships(role).map(relationship -> {
                Link relationshipWrapper = Link.create(relationship);
                return RolePlayer.create(roleWrapper, relationshipWrapper);
            });
        });
        return this.getConceptCollection(request, response, "relationships", buildThingGetter(request), collector);
    }

    private String getKeys(Request request, Response response) throws JsonProcessingException {
        return getAttributes(request, response, thing -> thing.keys());
    }

    private String getAttributes(Request request, Response response) throws JsonProcessingException {
        return getAttributes(request, response, thing -> thing.attributes());
    }

    private String getAttributes(Request request, Response response, Function<ai.grakn.concept.Thing,  Stream<Attribute<?>>> attributeFetcher) throws JsonProcessingException {
        int offset = getOffset(request);
        int limit = getLimit(request);

        Function<ai.grakn.concept.Thing, Stream<Jacksonisable>> collector = thing ->
                attributeFetcher.apply(thing).skip(offset).limit(limit).map(EmbeddedAttribute::create);

        return this.getConceptCollection(request, response, "attributes", buildThingGetter(request), collector);
    }

    private <X extends ai.grakn.concept.Concept> String getConceptCollection(
            Request request, Response response, String key,
            Function<GraknTx, X> getter, Function<X, Stream<Jacksonisable>> collector
    ) throws JsonProcessingException {
        response.type(APPLICATION_JSON);

        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));

        try (GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = labelGetTimer.time()) {
            X concept = getter.apply(tx);

            //If the concept was not found return;
            if(concept == null){
                response.status(SC_NOT_FOUND);
                return "[]";
            }

            List<Jacksonisable> list = collector.apply(concept).collect(Collectors.toList());
            Link link = Link.create(request.pathInfo());

            ListResource<Jacksonisable> listResource = ListResource.create(link, key, list);

            return objectMapper.writeValueAsString(listResource);
        }
    }

    private String getSchemaConceptSubs(Request request, Response response) throws JsonProcessingException {
        Function<ai.grakn.concept.SchemaConcept, Stream<Jacksonisable>> collector = schema -> schema.subs().map(ConceptBuilder::build);
        return getConceptCollection(request, response, "subs", buildSchemaConceptGetter(request), collector);
    }

    private String getTypeInstances(Request request, Response response) throws JsonProcessingException {
        response.type(APPLICATION_JSON);

        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        Label label = Label.of(mandatoryPathParameter(request, LABEL_PARAMETER));

        int offset = getOffset(request);
        int limit = getLimit(request);

        try (GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = instancesGetTimer.time()) {
            Type type = tx.getType(label);

            if(type == null){
                response.status(SC_NOT_FOUND);
                return "";
            }

            //Get the wrapper
            Things things = ConceptBuilder.buildThings(type, offset, limit);
            response.status(SC_OK);
            return objectMapper.writeValueAsString(things);
        }
    }

    private int getOffset(Request request){
        return getIntegerQueryParameter(request, OFFSET_PARAMETER, 0);
    }

    private int getLimit(Request request){
        return getIntegerQueryParameter(request, LIMIT_PARAMETER, 100);
    }

    private int getIntegerQueryParameter(Request request, String parameter, int defaultValue){
        String value = queryParameter(request, parameter);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    private String getSchemaByLabel(Request request, Response response) throws JsonProcessingException {
        Requests.validateRequest(request, APPLICATION_ALL, APPLICATION_JSON);
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        Label label = Label.of(mandatoryPathParameter(request, LABEL_PARAMETER));
        return getConcept(response, keyspace, (tx) -> tx.getSchemaConcept(label));
    }

    private String getConceptById(Request request, Response response) throws JsonProcessingException {
        Requests.validateRequest(request, APPLICATION_ALL, APPLICATION_JSON);
        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));
        ConceptId conceptId = ConceptId.of(mandatoryPathParameter(request, ID_PARAMETER));
        return getConcept(response, keyspace, (tx) -> tx.getConcept(conceptId));
    }

    private String getConcept(Response response, Keyspace keyspace, Function<GraknTx, ai.grakn.concept.Concept> getter) throws JsonProcessingException {
        response.type(APPLICATION_JSON);

        try (GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = conceptIdGetTimer.time()) {
            ai.grakn.concept.Concept concept = getter.apply(tx);

            if(concept != null){
                response.status(SC_OK);
                return objectMapper.writeValueAsString(ConceptBuilder.build(concept));
            } else {
                response.status(SC_NOT_FOUND);
                return "";
            }
        }
    }

    private String getTypes(Request request, Response response) throws JsonProcessingException {
        return getConcepts(request, response, "types", (tx) -> tx.admin().getMetaConcept().subs());
    }

    private String getRules(Request request, Response response) throws JsonProcessingException {
        return getConcepts(request, response, "rules", (tx) -> tx.admin().getMetaRule().subs());
    }

    private String getRoles(Request request, Response response) throws JsonProcessingException {
        return getConcepts(request, response, "roles", (tx) -> tx.admin().getMetaRole().subs());
    }

    private String getConcepts(
            Request request, Response response, String key,
            Function<GraknTx, Stream<? extends ai.grakn.concept.Concept>> getter
    ) throws JsonProcessingException {
        response.type(APPLICATION_JSON);

        Keyspace keyspace = Keyspace.of(mandatoryPathParameter(request, KEYSPACE_PARAM));

        try (GraknTx tx = factory.tx(keyspace, READ); Timer.Context context = labelGetTimer.time()) {
            List<Concept> concepts = getter.apply(tx).map(ConceptBuilder::<Concept>build).collect(Collectors.toList());
            ListResource list = ListResource.create(Requests.selfLink(request), key, concepts);
            response.status(SC_OK);
            return objectMapper.writeValueAsString(list);
        }
    }

    /**
     * Helper method used to build a function which will get a {@link ai.grakn.concept.SchemaConcept} by {@link Label}
     *
     * @param request The request which contains the {@link Label}
     * @return a function which can retrieve a {@link ai.grakn.concept.SchemaConcept} by {@link Label}
     */
    private static Function<GraknTx, ai.grakn.concept.SchemaConcept> buildSchemaConceptGetter(Request request){
        Label label = Label.of(mandatoryPathParameter(request, LABEL_PARAMETER));
        return tx -> tx.getSchemaConcept(label);
    }

    /**
     * Helper method used to build a function which will get a {@link ai.grakn.concept.Type} by {@link Label}
     *
     * @param request The request which contains the {@link Label}
     * @return a function which can retrieve a {@link ai.grakn.concept.Type} by {@link Label}
     */
    private static Function<GraknTx, ai.grakn.concept.Type> buildTypeGetter(Request request){
        Label label = Label.of(mandatoryPathParameter(request, LABEL_PARAMETER));
        return tx -> tx.getType(label);
    }

    /**
     * Helper method used to build a function which will get a {@link ai.grakn.concept.Thing} by {@link ConceptId}
     *
     * @param request The request which contains the {@link ConceptId}
     * @return a function which can retrieve a {@link ai.grakn.concept.Thing} by {@link ConceptId}
     */
    private static Function<GraknTx, ai.grakn.concept.Thing> buildThingGetter(Request request){
        ConceptId conceptId = ConceptId.of(mandatoryPathParameter(request, ID_PARAMETER));
        return tx -> {
            ai.grakn.concept.Concept concept = tx.getConcept(conceptId);
            if(concept == null || !concept.isThing()) return null;
            return concept.asThing();
        };
    }
}
