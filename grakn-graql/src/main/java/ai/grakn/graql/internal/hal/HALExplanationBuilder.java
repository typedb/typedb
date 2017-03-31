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

import ai.grakn.concept.Concept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.OUTBOUND_EDGE;

/**
 * Class for building HAL representations of a {@link Concept} or a {@link MatchQuery}.
 *
 * @author Marco Scoppetta
 */
public class HALExplanationBuilder {

    private final static Logger LOG = LoggerFactory.getLogger(HALExplanationBuilder.class);
    private final static int MATCH_QUERY_FIXED_DEGREE = 0;
    private final static String ASSERTION_URL = REST.WebPath.GRAPH_MATCH_QUERY_URI + "?keyspace=%s&query=match %s $r %s %s; select $r;&limit=%s";
    private final static String HAS_ROLE_EDGE = "EMPTY-GRAKN-ROLE";


    public static Json renderLookUpExplanation(MatchQuery matchQuery, Map<VarName, Concept> graqlResult, String keyspace, RelationType relationType,int limit) {

        //For each VarAdmin containing a relation we store a map containing varnames associated to roletypes
        Map<VarAdmin, Map<VarName, String>> roleTypes = computeRoleTypesFromQuery(matchQuery);

        //Collect all the types explicitly asked in the match query
        Set<TypeName> typesAskedInQuery = matchQuery.admin().getTypes().stream().map(x -> x.asType().getName()).collect(Collectors.toSet());


        return buildHALRepresentations(graqlResult, typesAskedInQuery, roleTypes, keyspace, relationType, limit);
    }

    public static Json answersToHAL(Stream<Answer> answerStream, String keyspace, int limit) {
        final Json conceptsArray = Json.array();
        answerStream.forEach(answer -> {
            AnswerExplanation expl = answer.getExplanation();
            if (expl.isLookupExplanation()) {
                RelationType relationType = ((RelationType) ((ReasonerAtomicQuery) expl.getQuery()).getAtom().getType());
                renderLookUpExplanation(expl.getQuery().getMatchQuery(), answer.map(), keyspace, relationType, limit).asList().forEach(conceptsArray::add);
            } else if (expl.isRuleExplanation()) {
                Atom innerAtom = ((RuleExplanation) expl).getRule().getHead().getAtom();
                //TODO: handle case innerAtom isa resource
                if (innerAtom.isRelation()) {
                    RelationType relType = (RelationType) innerAtom.getType();
                    renderLookUpExplanation(((RuleExplanation) expl).getRule().getHead().getMatchQuery(), answer.map(), keyspace, relType, limit).asList().forEach(conceptsArray::add);
                }
                answersToHAL(expl.getAnswers().stream(), keyspace, limit).asList().forEach(conceptsArray::add);
            }
        });
        return conceptsArray;
    }

    private static Json buildHALRepresentations(Map<VarName, Concept> resultLine, Set<TypeName> typesAskedInQuery, Map<VarAdmin, Map<VarName, String>> roleTypes, String keyspace, RelationType relationType, int limit) {
        final Json lines = Json.array();
        Map<VarName, Representation> mapFromVarNameToHALObject = new HashMap<>();
        resultLine.entrySet().stream().filter(entry->entry.getValue().isInstance()).forEach(currentMapEntry -> {

            Concept currentConcept = currentMapEntry.getValue();
            if (currentConcept.isType() && currentConcept.asType().isImplicit()) return;

            LOG.trace("Building HAL resource for concept with id {}", currentConcept.getId().getValue());
            Representation currentHal = new HALConceptData(currentConcept, MATCH_QUERY_FIXED_DEGREE, false,
                    typesAskedInQuery, keyspace, 0, -1).getRepresentation();

            // Local map that will allow us to fetch HAL representation of roleplayers when populating _embedded of the generated relation (in loopThroughRelations)
            mapFromVarNameToHALObject.put(currentMapEntry.getKey(), currentHal);

            lines.add(Json.read(currentHal.toString(RepresentationFactory.HAL_JSON)));
        });
        // All the variables of current map have an HAL representation. Add _direction OUT
        mapFromVarNameToHALObject.values().forEach(hal -> hal.withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE));
        // Check if we need also to generate a "generated-relation" and embed in it all its role players' HAL representations
        loopThroughRelations(roleTypes, mapFromVarNameToHALObject, resultLine, relationType, keyspace, limit).asList().forEach(generatedRelation -> lines.add(generatedRelation));

        return lines;
    }

    static Json loopThroughRelations(Map<VarAdmin, Map<VarName, String>> roleTypes, Map<VarName, Representation> mapFromVarNameToHALObject, Map<VarName, Concept> resultLine, RelationType relationType,String keyspace,  int limit) {

        final Json generatedRelations = Json.array();
        // For each relation (VarAdmin key in roleTypes) we fetch all the role-players representations and embed them in the relation's HAL representation.
        roleTypes.entrySet().forEach(currentEntry -> {
            Collection<VarName> varNamesInCurrentRelation = currentEntry.getValue().keySet();
            // Chain Concept ids (sorted alphabetically) corresponding to varnames in current relation
            String idsList = varNamesInCurrentRelation.stream().map(key -> resultLine.get(key).getId().getValue()).sorted().collect(Collectors.joining(""));
            // Generated relation ID
            String relationId = "temp-assertion-" + idsList;
            // This string contains the match query to execute when double clicking on the node from Dashboard
            String relationHref = computeRelationHref(relationType, varNamesInCurrentRelation, resultLine, currentEntry.getValue(), keyspace, limit);
            Representation genRelation = new HALGeneratedRelation().getNewGeneratedRelationExplanation(relationId, relationHref, relationType);

            varNamesInCurrentRelation.forEach(varName -> genRelation.withRepresentation(currentEntry.getValue().get(varName), mapFromVarNameToHALObject.get(varName)));

            generatedRelations.add(genRelation.toString(RepresentationFactory.HAL_JSON));
        });
        return generatedRelations;
    }

    private static String computeRelationHref(RelationType relationType, Collection<VarName> varNamesInCurrentRelation, Map<VarName, Concept> resultLine, Map<VarName, String> roleTypes, String keyspace, int limit) {
        String isaString = (relationType != null) ? "isa " + relationType.getName().getValue() : "";
        StringBuilder stringBuilderVarsWithIds = new StringBuilder();
        StringBuilder stringBuilderParenthesis = new StringBuilder().append('(');
        char currentVarLetter = 'a';
        for (VarName varName : varNamesInCurrentRelation) {
            String id = resultLine.get(varName).getId().getValue();
            stringBuilderVarsWithIds.append(" $").append(currentVarLetter).append(" id '").append(id).append("';");
            String role = (roleTypes.get(varName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(varName) + ":";
            stringBuilderParenthesis.append(role).append("$").append(currentVarLetter++).append(",");
        }
        String varsWithIds = stringBuilderVarsWithIds.toString();
        String parenthesis = stringBuilderParenthesis.deleteCharAt(stringBuilderParenthesis.length() - 1).append(')').toString();

        return String.format(ASSERTION_URL, keyspace, varsWithIds, parenthesis, isaString, limit);
    }


    private static Map<VarAdmin, Map<VarName, String>> computeRoleTypesFromQuery(MatchQuery matchQuery) {
        final Map<VarAdmin, Map<VarName, String>> roleTypes = new HashMap<>();
        matchQuery.admin().getPattern().getVars().forEach(var -> {
            if (var.getProperty(RelationProperty.class).isPresent() && !var.isUserDefinedName()) {
                roleTypes.put(var, new HashMap<>());
                var.getProperty(RelationProperty.class).get()
                        .getRelationPlayers().forEach(x ->
                        roleTypes.get(var).put(x.getRolePlayer().getVarName(),
                                (x.getRoleType().isPresent()) ? x.getRoleType().get().getPrintableName() : HAS_ROLE_EDGE)
                );
            }
        });
        return roleTypes;
    }
}
