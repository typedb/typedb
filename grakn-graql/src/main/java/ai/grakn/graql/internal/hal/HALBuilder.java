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
import ai.grakn.concept.TypeName;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.util.StringConverter;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.OUTBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.computeRoleTypesFromQuery;

/**
 * Class for building HAL representations of a {@link Concept} or a {@link MatchQuery}.
 *
 * @author Marco Scoppetta
 */
public class HALBuilder {

    private final static Logger LOG = LoggerFactory.getLogger(HALBuilder.class);
    private final static int MATCH_QUERY_FIXED_DEGREE = 0;
    private final static String ASSERTION_URL = REST.WebPath.GRAPH_MATCH_QUERY_URI + "?keyspace=%s&query=match %s $r %s %s; select $r;&limit=%s";
    private final static String HAS_ROLE_EDGE = "EMPTY-GRAKN-ROLE";


    public static Json renderHALArrayData(MatchQuery matchQuery, Collection<Map<VarName, Concept>> graqlResultsList, String keyspace, int offset, int limit) {

        //For each VarAdmin containing a relation we store a map containing varNames associated to RoleTypes
        Map<VarAdmin, Map<VarName, String>> roleTypes = computeRoleTypesFromQuery(matchQuery);

        //Collect all the types explicitly asked in the match query
        Set<TypeName> typesAskedInQuery = matchQuery.admin().getTypes().stream().map(x -> x.asType().getName()).collect(Collectors.toSet());

        return buildHALRepresentations(graqlResultsList, typesAskedInQuery, roleTypes, keyspace, offset, limit);
    }

    public static String renderHALConceptData(Concept concept, int separationDegree, String keyspace, int offset, int limit) {
        return new HALConceptData(concept, separationDegree, false, new HashSet<>(), keyspace, offset, limit).render();
    }

    public static String HALExploreConcept(Concept concept, String keyspace, int offset, int limit) {
        String renderedHAL = null;

        if (concept.isInstance()) {
            renderedHAL = new HALExploreInstance(concept, keyspace, offset, limit).render();
        }
        if (concept.isType()) {
            renderedHAL = new HALExploreType(concept, keyspace, offset, limit).render();
        }

        return renderedHAL;
    }

    private static Json buildHALRepresentations(Collection<Map<VarName, Concept>> graqlResultsList, Set<TypeName> typesAskedInQuery, Map<VarAdmin, Map<VarName, String>> roleTypes, String keyspace, int offset, int limit) {
        final Json lines = Json.array();
        graqlResultsList.forEach(resultLine -> {
            Map<VarName, Representation> mapFromVarNameToHALObject = new HashMap<>();
            resultLine.entrySet().forEach(currentMapEntry -> {

                Concept currentConcept = currentMapEntry.getValue();
                if (currentConcept.isType() && currentConcept.asType().isImplicit()) return;

                LOG.trace("Building HAL resource for concept with id {}", currentConcept.getId().getValue());
                Representation currentHal = new HALConceptData(currentConcept, MATCH_QUERY_FIXED_DEGREE, true,
                        typesAskedInQuery, keyspace, offset, limit).getRepresentation();

                // Local map that will allow us to fetch HAL representation of RolePlayers when populating _embedded of the generated relation (in loopThroughRelations)
                mapFromVarNameToHALObject.put(currentMapEntry.getKey(), currentHal);

                lines.add(Json.read(currentHal.toString(RepresentationFactory.HAL_JSON)));
            });
            // All the variables of current map have an HAL representation. Add _direction OUT
            mapFromVarNameToHALObject.values().forEach(hal -> hal.withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE));
            // Check if we need also to generate a "generated-relation" and embed in it all its role players' HAL representations
            loopThroughRelations(roleTypes, mapFromVarNameToHALObject, resultLine, keyspace, limit).asList().forEach(generatedRelation -> lines.add(generatedRelation));
        });
        return lines;
    }

    static Json loopThroughRelations(Map<VarAdmin, Map<VarName, String>> roleTypes, Map<VarName, Representation> mapFromVarNameToHALObject, Map<VarName, Concept> resultLine, String keyspace, int limit) {

        final Json generatedRelations = Json.array();
        // For each relation (VarAdmin key in roleTypes) we fetch all the role-players representations and embed them in the generated-relation's HAL representation.
        roleTypes.entrySet().forEach(currentEntry -> {
            Collection<VarName> varNamesInCurrentRelation = currentEntry.getValue().keySet();
            // Chain Concept ids (sorted alphabetically) corresponding to varnames in current relation
            String idsList = varNamesInCurrentRelation.stream().map(key -> resultLine.get(key).getId().getValue()).sorted().collect(Collectors.joining(""));
            // Generated relation ID
            String relationId = "temp-assertion-" + idsList;
            final Optional<TypeName> relationType = currentEntry.getKey().getProperty(IsaProperty.class).flatMap(x -> x.getType().getTypeName());
            // This string contains the match query to execute when double clicking on the 'generated-relation' node from Dashboard
            String relationHref = computeRelationHref(relationType, varNamesInCurrentRelation, resultLine, currentEntry.getValue(), keyspace, limit);
            // Create HAL representation of generated relation
            Representation genRelation = new HALGeneratedRelation().getNewGeneratedRelation(relationId, relationHref, relationType);
            // Embed each role player's HAL representation in the current relation _embedded
            varNamesInCurrentRelation.forEach(varName -> genRelation.withRepresentation(currentEntry.getValue().get(varName), mapFromVarNameToHALObject.get(varName)));

            generatedRelations.add(genRelation.toString(RepresentationFactory.HAL_JSON));
        });
        return generatedRelations;
    }

    private static String computeRelationHref(Optional<TypeName> relationType, Collection<VarName> varNamesInCurrentRelation, Map<VarName, Concept> resultLine, Map<VarName, String> roleTypes, String keyspace, int limit) {
        String isaString = (relationType.isPresent()) ? "isa " + StringConverter.typeNameToString(relationType.get()) : "";
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
}