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

package ai.grakn.graql.internal.hal;

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.hal.HALUtils.BASETYPE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.HAS_EMPTY_ROLE_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.INFERRED_RELATIONSHIP;
import static ai.grakn.graql.internal.hal.HALUtils.LINKS_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.OUTBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.buildInferredRelationshipsMap;
import static ai.grakn.graql.internal.hal.HALUtils.computeRoleTypesFromQuery;
import static java.util.stream.Collectors.toSet;

/**
 * Class for building HAL representations of a {@link Concept} or a {@link Match}.
 *
 * @author Marco Scoppetta
 */
public class HALBuilder {

    private final static Logger LOG = LoggerFactory.getLogger(HALBuilder.class);
    private final static int MATCH_QUERY_FIXED_DEGREE = 0;
    private final static String ASSERTION_URL = "?keyspace=%s&query=match %s %s %s %s; %s &limitEmbedded=%s&infer=false&materialise=false";


    public static Json renderHALArrayData(GetQuery getQuery, int offset, int limit) {
        Collection<Answer> answers = getQuery.execute();
        return renderHALArrayData(getQuery, answers, offset, limit, false);
    }

    public static Json renderHALArrayData(GetQuery getQuery, Collection<Answer> results, int offset, int limit, boolean filterInstances) {
        Keyspace keyspace = getQuery.tx().get().getKeyspace();

        //For each VarPatterAdmin containing a relationship we store a map containing varNames associated to RoleTypes
        Map<VarPatternAdmin, Pair<Map<Var, String>, String>> roleTypes = new HashMap<>();
        if (results.iterator().hasNext()) {
            // Compute map on first answer in result, since it will be the same for all the answers
            roleTypes = computeRoleTypesFromQuery(getQuery, results.iterator().next());
        }
        //Collect all the types explicitly asked in the get query
        Set<Label> typesAskedInQuery = getQuery.match().admin().getSchemaConcepts().stream().map(SchemaConcept::getLabel).collect(toSet());

        return buildHALRepresentations(results, typesAskedInQuery, roleTypes, keyspace, offset, limit, filterInstances);
    }

    public static String renderHALConceptData(Concept concept, int separationDegree, Keyspace keyspace, int offset, int limit) {
        return new HALConceptData(concept, separationDegree, false, new HashSet<>(), keyspace, offset, limit).render();
    }

    @Nullable
    public static String HALExploreConcept(Concept concept, Keyspace keyspace, int offset, int limit) {
        String renderedHAL = null;

        if (concept.isThing()) {
            renderedHAL = new HALExploreInstance(concept, keyspace, offset, limit).render();
        }
        if (concept.isSchemaConcept()) {
            renderedHAL = new HALExploreSchemaConcept(concept, keyspace, offset, limit).render();
        }

        return renderedHAL;
    }

    public static Json explanationAnswersToHAL(Stream<Answer> answerStream, int limit) {
        final Json conceptsArray = Json.array();
        answerStream.forEach(answer -> {
            AnswerExplanation expl = answer.getExplanation();
            if (expl.isLookupExplanation()) {
                HALBuilder.renderHALArrayData(expl.getQuery().getQuery(), Collections.singletonList(answer), 0, limit, true).asList().forEach(conceptsArray::add);
            } else if (expl.isRuleExplanation()) {
                Atom innerAtom = ((RuleExplanation) expl).getRule().getHead().getAtom();
                //TODO: handle case innerAtom isa resource
                if (innerAtom.isRelation()) {
                    HALBuilder.renderHALArrayData(expl.getQuery().getQuery(), Collections.singletonList(answer), 0, limit, true).asList().forEach(conceptsArray::add);
                }
                explanationAnswersToHAL(expl.getAnswers().stream(), limit).asList().forEach(conceptsArray::add);
            }
        });
        return conceptsArray;
    }

    private static Json buildHALRepresentations(Collection<Answer> graqlResultsList, Set<Label> typesAskedInQuery, Map<VarPatternAdmin, Pair<Map<Var, String>, String>> roleTypes, Keyspace keyspace, int offset, int limit, boolean filterInstances) {
        final Json lines = Json.array();
        graqlResultsList.forEach(answer -> {
            Map<VarPatternAdmin, Boolean> inferredRelationships = buildInferredRelationshipsMap(answer);
            Map<Var, Representation> mapFromVarNameToHALObject = new HashMap<>();
            Stream<Map.Entry<Var, Concept>> entriesStream = answer.map().entrySet().stream();
            // Filter to work only with Instances when building HAL for explanation tree from Reasoner
            if (filterInstances) entriesStream = entriesStream.filter(entry -> entry.getValue().isThing());
            entriesStream.forEach(currentMapEntry -> {
                Concept currentConcept = currentMapEntry.getValue();

                LOG.trace("Building HAL resource for concept with id {}", currentConcept.getId().getValue());
                Representation currentHal = new HALConceptData(currentConcept, MATCH_QUERY_FIXED_DEGREE, true,
                        typesAskedInQuery, keyspace, offset, limit).getRepresentation();


                // Local map that will allow us to fetch HAL representation of RolePlayers when populating _embedded of the generated relationship (in loopThroughRelationships)
                mapFromVarNameToHALObject.put(currentMapEntry.getKey(), currentHal);

                Json jsonRepresentation = Json.read(currentHal.toString(RepresentationFactory.HAL_JSON));
                // If current concept is a relationship obtained with inference (and we are not building an explanation response) override Explore URL and BaseType
                if(!answer.getExplanation().isEmpty() && currentConcept.isRelationship() && !filterInstances){
                    jsonRepresentation.set(BASETYPE_PROPERTY, INFERRED_RELATIONSHIP);
                    jsonRepresentation.at(LINKS_PROPERTY).set("self",Json.object().set("href", computeHrefInferred(currentConcept, keyspace, limit)));
                }

                lines.add(jsonRepresentation);
            });
            // All the variables of current map have an HAL representation. Add _direction OUT
            mapFromVarNameToHALObject.values().forEach(hal -> hal.withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE));
            // Check if we need also to generate a "generated-relationship" and embed in it all its role players' HAL representations
            loopThroughRelationships(roleTypes, mapFromVarNameToHALObject, answer.map(), keyspace, limit, inferredRelationships).forEach(generatedRelationship ->
                    lines.add(Json.read(generatedRelationship.toString(RepresentationFactory.HAL_JSON))));
        });
        return lines;
    }

    private static String computeHrefInferred(Concept currentConcept, Keyspace keyspace, int limit){
        Set<Thing> thingSet = new HashSet<>();
        currentConcept.asRelationship().allRolePlayers().values().forEach(set -> set.forEach(thingSet::add));
        String isaString =  "isa " + currentConcept.asRelationship().type().getLabel();
        StringBuilder stringBuilderVarsWithIds = new StringBuilder();
        StringBuilder stringBuilderParenthesis = new StringBuilder().append('(');
        char currentVarLetter = 'a';
        for (Thing var : thingSet) {
            stringBuilderVarsWithIds.append(" $").append(currentVarLetter).append(" id '").append(var.getId().getValue()).append("';");
            stringBuilderParenthesis.append("$").append(currentVarLetter++).append(",");
        }
        String varsWithIds = stringBuilderVarsWithIds.toString();
        String parenthesis = stringBuilderParenthesis.deleteCharAt(stringBuilderParenthesis.length() - 1).append(')').toString();

        String withoutUrl = String.format(ASSERTION_URL, keyspace, varsWithIds, "", parenthesis, isaString, "get;", limit);

        String URL = REST.WebPath.Dashboard.EXPLAIN;

        return URL + withoutUrl;

    }

    private static Collection<Representation> loopThroughRelationships(Map<VarPatternAdmin, Pair<Map<Var, String>, String>> roleTypes, Map<Var, Representation> mapFromVarNameToHALObject, Map<Var, Concept> resultLine, Keyspace keyspace, int limit, Map<VarPatternAdmin, Boolean> inferredRelationships) {

        final Collection<Representation> generatedRelationships = new ArrayList<>();
        // For each relation (VarPatternAdmin key in roleTypes) we fetch all the role-players representations and embed them in the generated-relationship's HAL representation.
        roleTypes.entrySet().forEach(currentEntry -> {
            Collection<Var> varNamesInCurrentRelationship = currentEntry.getValue().getKey().keySet();
            // Chain Concept ids (sorted alphabetically) corresponding to varNames in current relationship
            String idsList = varNamesInCurrentRelationship.stream().map(key -> resultLine.get(key).getId().getValue()).sorted().collect(Collectors.joining(""));
            // Generated relationship ID
            String relationshipId = "temp-assertion-" + idsList;
            String relationshipType = currentEntry.getValue().getValue();
            boolean isInferred = inferredRelationships.containsKey(currentEntry.getKey()) && inferredRelationships.get(currentEntry.getKey());
            // This string contains the get query to execute when double clicking on the 'generated-relation' node from Dashboard
            // It will be an 'explain-query' if the current relation is inferred
            String relationshipHref = computeRelationshipHref(relationshipType, varNamesInCurrentRelationship, resultLine, currentEntry.getValue().getKey(), keyspace, limit, isInferred);
            // Create HAL representation of generated relation
            Representation genRelationship = new HALGeneratedRelationship().getNewGeneratedRelationship(relationshipId, relationshipHref, relationshipType, isInferred);
            // Embed each role player's HAL representation in the current relation _embedded
            varNamesInCurrentRelationship.forEach(varName -> genRelationship.withRepresentation(currentEntry.getValue().getKey().get(varName), mapFromVarNameToHALObject.get(varName)));

            generatedRelationships.add(genRelationship);
        });
        return generatedRelationships;
    }

    private static String computeRelationshipHref(String relationshipType, Collection<Var> varNamesInCurrentRelationship, Map<Var, Concept> resultLine, Map<Var, String> varNameToRole, Keyspace keyspace, int limit, boolean isInferred) {
        String isaString = (!relationshipType.equals("")) ? "isa " + relationshipType : "";
        StringBuilder stringBuilderVarsWithIds = new StringBuilder();
        StringBuilder stringBuilderParenthesis = new StringBuilder().append('(');
        char currentVarLetter = 'a';
        for (Var varName : varNamesInCurrentRelationship) {
            String id = resultLine.get(varName).getId().getValue();
            stringBuilderVarsWithIds.append(" $").append(currentVarLetter).append(" id '").append(id).append("';");
            String role = (varNameToRole.get(varName).equals(HAS_EMPTY_ROLE_EDGE)) ? "" : varNameToRole.get(varName) + ":";
            stringBuilderParenthesis.append(role).append("$").append(currentVarLetter++).append(",");
        }
        String varsWithIds = stringBuilderVarsWithIds.toString();
        String parenthesis = stringBuilderParenthesis.deleteCharAt(stringBuilderParenthesis.length() - 1).append(')').toString();

        String dollarR = (isInferred) ? "" : "$r";
        String getR = (isInferred) ? "get;" : "get $r;";

        String withoutUrl = String.format(ASSERTION_URL, keyspace, varsWithIds, dollarR, parenthesis, isaString, getR, limit);

        String URL = (isInferred) ? REST.WebPath.Dashboard.EXPLAIN : REST.WebPath.KB.GRAQL;

        return URL + withoutUrl;
    }
}