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
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HALConceptRepresentationBuilder {

    private final static Logger LOG = LoggerFactory.getLogger(HALConceptRepresentationBuilder.class);
    private final static int MATCH_QUERY_FIXED_DEGREE = 0;
    private final static String ASSERTION_URL = REST.WebPath.GRAPH_MATCH_QUERY_URI + "?query=match $x id '%s'; $y id '%s'; $r (%s$x, %s$y); select $r;";
    private final static String HAS_ROLE_EDGE = "EMPTY-GRAKN-ROLE";

    public static Json renderHALArrayData(MatchQuery matchQuery, Collection<Map<String, Concept>> graqlResultsList, String rootConceptId) {
        //Compute connections between variables in Graql result
        Map<String, Collection<String>> linkedNodes = new HashMap<>();
        Map<String, String> roleTypes = new HashMap<>();
        computeLinkedNodesFromQuery(matchQuery, linkedNodes, roleTypes);
        //Collect all the types explicitly asked in the match query
        Set<String> typesAskedInQuery = matchQuery.admin().getTypes().stream().map(x->x.asType().getName()).collect(Collectors.toSet());
        //Check if among the types asked in the query there is a relation-type (we only support one relation-type per query)
        String relationType = matchQuery.admin().getTypes().stream()
                .filter(Concept::isRelationType)
                .findFirst().map(Concept::getId)
                .orElse("");

        return buildHALRepresentations(graqlResultsList, linkedNodes, typesAskedInQuery, relationType, roleTypes, rootConceptId);
    }

    public static String renderHALConceptData(Concept concept, int separationDegree) {
        return new HALConceptData(concept, separationDegree, false, new HashSet<>()).render();
    }

    public static String renderHALConceptOntology(Concept concept, String rootConceptId) {
        return new HALConceptOntology(concept, rootConceptId).render();
    }

    private static Json buildHALRepresentations(Collection<Map<String, Concept>> graqlResultsList, Map<String, Collection<String>> linkedNodes, Set<String> typesAskedInQuery, String relationType, Map<String, String> roleTypes, String rootConceptId) {
        final Json lines = Json.array();
        graqlResultsList.parallelStream()
                .forEach(resultLine -> resultLine.entrySet().forEach(current -> {

                    if (current.getValue().isType() && current.getValue().asType().isImplicit()) return;

                    LOG.trace("Building HAL resource for concept with id {}", current.getValue().getId());
                    Representation currentHal = new HALConceptData(current.getValue(), MATCH_QUERY_FIXED_DEGREE, true,
                            typesAskedInQuery).getRepresentation();
                    attachGeneratedRelation(currentHal, current, linkedNodes, resultLine, relationType, roleTypes);
                    lines.add(Json.read(currentHal.toString(RepresentationFactory.HAL_JSON)));

                }));
        return lines;
    }

    private static void attachGeneratedRelation(Representation currentHal, Map.Entry<String, Concept> current, Map<String, Collection<String>> linkedNodes, Map<String, Concept> resultLine, String relationType, Map<String, String> roleTypes) {
        if (linkedNodes.containsKey(current.getKey())) {
            linkedNodes.get(current.getKey())
                    .forEach(varName -> {
                        if (current.getValue() != null) {
                            Concept rolePlayer = resultLine.get(varName);
                            String currentID = current.getValue().getId();

                            String firstID;
                            String secondID;
                            String firstRole;
                            String secondRole;

                            if (currentID.compareTo(rolePlayer.getId()) > 0) {
                                firstID = currentID;
                                secondID = rolePlayer.getId();
                                firstRole = (roleTypes.get(current.getKey()).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(current.getKey()) + ":";
                                secondRole = (roleTypes.get(varName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(varName) + ":";
                            } else {
                                firstID = rolePlayer.getId();
                                secondID = currentID;
                                secondRole = (roleTypes.get(current.getKey()).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(current.getKey()) + ":";
                                firstRole = (roleTypes.get(varName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(varName) + ":";
                            }

                            String assertionID = String.format(ASSERTION_URL, firstID, secondID, firstRole, secondRole);
                            currentHal.withRepresentation(roleTypes.get(current.getKey()), new HALGeneratedRelation().getNewGeneratedRelation(assertionID, relationType));
                        }
                    });
        }
    }

    private static Map<String, Collection<String>> computeLinkedNodesFromQuery(MatchQuery matchQuery, Map<String, Collection<String>> linkedNodes, Map<String, String> roleTypes) {
        matchQuery.admin().getPattern().getVars().forEach(var -> {
            //if in the current var is expressed some kind of relation (e.g. ($x,$y))
            if (var.getProperty(RelationProperty.class).isPresent()) {
                //collect all the role players in the current var's relations (e.g. 'x' and 'y')
                final List<String> rolePlayersInVar = new ArrayList<>();
                        var.getProperty(RelationProperty.class).get()
                        .getRelationPlayers().map(x -> {
                            roleTypes.put(x.getRolePlayer().getVarName(),
                                    (x.getRoleType().isPresent()) ? x.getRoleType().get().getPrintableName() : HAS_ROLE_EDGE);
                            return x.getRolePlayer().getVarName();
                        }).forEach(result -> {rolePlayersInVar.add(result);});
                //if it is a binary or ternary relation
                if (rolePlayersInVar.size() > 1) {
                    rolePlayersInVar.forEach(rolePlayer -> {
                        linkedNodes.putIfAbsent(rolePlayer, new HashSet<>());
                        rolePlayersInVar.forEach(y -> {
                            if (!y.equals(rolePlayer))
                                linkedNodes.get(rolePlayer).add(y);
                        });
                    });
                }
            }
        });
        return linkedNodes;
    }
}
