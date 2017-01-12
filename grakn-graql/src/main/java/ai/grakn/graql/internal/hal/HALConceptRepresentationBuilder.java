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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.RelationPlayer;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
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
    private final static String ASSERTION_URL = REST.WebPath.GRAPH_MATCH_QUERY_URI + "?keyspace=%s&query=match $x id '%s'; $y id '%s'; $r (%s$x, %s$y) %s; select $r;";
    private final static String HAS_ROLE_EDGE = "EMPTY-GRAKN-ROLE";

    public static Json renderHALArrayData(MatchQuery matchQuery, Collection<Map<VarName, Concept>> graqlResultsList, String keyspace) {

        //Stores connections between variables in Graql result [varName:List<VarAdmin> (only VarAdmins that contain a relation)]
        Map<VarName, Collection<VarAdmin>> linkedNodes =  computeLinkedNodesFromQuery(matchQuery);
        //For each VarAdmin(hashCode) containing a relation we store a map containing varnames associated to roletypes
        Map<String,Map<VarName, String>> roleTypes = computeRoleTypesFromQuery(matchQuery);


        //Collect all the types explicitly asked in the match query
        Set<TypeName> typesAskedInQuery = matchQuery.admin().getTypes().stream().map(x -> x.asType().getName()).collect(Collectors.toSet());


        return buildHALRepresentations(graqlResultsList, linkedNodes, typesAskedInQuery, roleTypes, keyspace);
    }

    public static String renderHALConceptData(Concept concept, int separationDegree, String keyspace) {
        return new HALConceptData(concept, separationDegree, false, new HashSet<>(), keyspace).render();
    }

    public static String renderHALConceptOntology(Concept concept, String keyspace) {
        return new HALConceptOntology(concept, keyspace).render();
    }

    private static Json buildHALRepresentations(Collection<Map<VarName, Concept>> graqlResultsList, Map<VarName, Collection<VarAdmin>> linkedNodes, Set<TypeName> typesAskedInQuery, Map<String,Map<VarName, String>> roleTypes, String keyspace) {
        final Json lines = Json.array();
        graqlResultsList.forEach(resultLine -> resultLine.entrySet().forEach(current -> {

            if (current.getValue().isType() && current.getValue().asType().isImplicit()) return;

            LOG.trace("Building HAL resource for concept with id {}", current.getValue().getId().getValue());
            Representation currentHal = new HALConceptData(current.getValue(), MATCH_QUERY_FIXED_DEGREE, true,
                    typesAskedInQuery, keyspace).getRepresentation();
            attachGeneratedRelations(currentHal, current, linkedNodes, resultLine, roleTypes, keyspace);
            lines.add(Json.read(currentHal.toString(RepresentationFactory.HAL_JSON)));

        }));
        return lines;
    }

    private static void attachGeneratedRelations(Representation currentHal, Map.Entry<VarName, Concept> current, Map<VarName, Collection<VarAdmin>> linkedNodes, Map<VarName, Concept> resultLine, Map<String,Map<VarName, String>> roleTypes, String keyspace) {
        if (linkedNodes.containsKey(current.getKey())) {
            linkedNodes.get(current.getKey())
                    .forEach(currentRelation -> {
                        if (current.getValue() != null) {
                            VarName currentVarName = current.getKey();
                            Concept currentRolePlayer = current.getValue();
                            final TypeName relationType = currentRelation.getProperty(IsaProperty.class).flatMap(x->x.getType().getTypeName()).orElse(null);

                            currentRelation.getProperty(RelationProperty.class).get()
                                    .getRelationPlayers()
                                    //get all the other vars(rolePlayers) contained in the relation
                                    .filter(x -> (!x.getRolePlayer().getVarName().equals(currentVarName)))
                                    .map(RelationPlayer::getRolePlayer).forEach(otherVar -> {

                                if(resultLine.get(otherVar.getVarName())!=null) {
                                    attachSingleGeneratedRelation(currentHal, currentRolePlayer, resultLine.get(otherVar.getVarName()), roleTypes.get(String.valueOf(currentRelation.hashCode())), currentVarName, otherVar.getVarName(), relationType, keyspace);
                                }
                            });

                        }
                    });
        }
    }

    private static void attachSingleGeneratedRelation(Representation currentHal, Concept currentVar, Concept otherVar, Map<VarName, String> roleTypes, VarName currentVarName, VarName otherVarName, TypeName relationType, String keyspace) {
        ConceptId currentID = currentVar.getId();

        ConceptId firstID;
        ConceptId secondID;
        String firstRole;
        String secondRole;

        if (currentID.compareTo(otherVar.getId()) > 0) {
            firstID = currentID;
            secondID = otherVar.getId();
            firstRole = (roleTypes.get(currentVarName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(currentVarName) + ":";
            secondRole = (roleTypes.get(otherVarName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(otherVarName) + ":";
        } else {
            firstID = otherVar.getId();
            secondID = currentID;
            secondRole = (roleTypes.get(currentVarName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(currentVarName) + ":";
            firstRole = (roleTypes.get(otherVarName).equals(HAS_ROLE_EDGE)) ? "" : roleTypes.get(otherVarName) + ":";
        }

        String isaString = (relationType==null) ? "" : "isa "+relationType.getValue();

        String assertionID = String.format(ASSERTION_URL, keyspace, firstID, secondID, firstRole, secondRole,isaString);
        currentHal.withRepresentation(roleTypes.get(currentVarName), new HALGeneratedRelation().getNewGeneratedRelation(assertionID, relationType));
    }

    private static Map<VarName, Collection<VarAdmin>> computeLinkedNodesFromQuery(MatchQuery matchQuery) {
        final Map<VarName, Collection<VarAdmin>> linkedNodes = new HashMap<>();
        matchQuery.admin().getPattern().getVars().forEach(var -> {
            //if in the current var is expressed some kind of relation (e.g. ($x,$y))
            if (var.getProperty(RelationProperty.class).isPresent()) {
                //collect all the role players in the current var's relations (e.g. 'x' and 'y')
                final List<VarName> rolePlayersInVar = new ArrayList<>();
                var.getProperty(RelationProperty.class).get()
                        .getRelationPlayers().map(x -> x.getRolePlayer().getVarName()).forEach(rolePlayersInVar::add);
                //if it is a binary or ternary relation
                if (rolePlayersInVar.size() > 1) {
                    rolePlayersInVar.forEach(rolePlayer -> {
                        linkedNodes.putIfAbsent(rolePlayer, new HashSet<>());
                        rolePlayersInVar.forEach(y -> {
                            if (!y.equals(rolePlayer)) {
                                linkedNodes.get(rolePlayer).add(var);
                            }
                        });
                    });
                }
            }
        });
        return linkedNodes;
    }

    private static Map<String,Map<VarName,String>> computeRoleTypesFromQuery(MatchQuery matchQuery) {
        final Map<String,Map<VarName,String>> roleTypes = new HashMap<>();
        matchQuery.admin().getPattern().getVars().forEach(var -> {
            if (var.getProperty(RelationProperty.class).isPresent()) {
                final String varHashCode =String.valueOf(var.hashCode());
                roleTypes.put(varHashCode,new HashMap<>());
                var.getProperty(RelationProperty.class)
                        .get()
                        .getRelationPlayers()
                        .forEach(x -> roleTypes.get(varHashCode).put(x.getRolePlayer().getVarName(),
                                (x.getRoleType().isPresent()) ? x.getRoleType().get().getPrintableName() : HAS_ROLE_EDGE));
            }
        });
        return roleTypes;
    }

    static Schema.BaseType getBaseType(Instance instance) {
        if (instance.isEntity()) {
            return Schema.BaseType.ENTITY;
        } else if (instance.isRelation()) {
            return Schema.BaseType.RELATION;
        } else if (instance.isResource()) {
            return Schema.BaseType.RESOURCE;
        } else if (instance.isRule()) {
            return Schema.BaseType.RULE;
        } else {
            throw new RuntimeException("Unrecognized base type of " + instance);
        }
    }

    static Schema.BaseType getBaseType(Type type) {
        if (type.isEntityType()) {
            return Schema.BaseType.ENTITY_TYPE;
        } else if (type.isRelationType()) {
            return Schema.BaseType.RELATION_TYPE;
        } else if (type.isResourceType()) {
            return Schema.BaseType.RESOURCE_TYPE;
        } else if (type.isRuleType()) {
            return Schema.BaseType.RULE_TYPE;
        } else if (type.isRoleType()) {
            return Schema.BaseType.ROLE_TYPE;
        } else if (type.getName().equals(Schema.MetaSchema.CONCEPT.getName())) {
            return Schema.BaseType.TYPE;
        } else {
            throw new RuntimeException("Unrecognized base type of " + type);
        }
    }
}
