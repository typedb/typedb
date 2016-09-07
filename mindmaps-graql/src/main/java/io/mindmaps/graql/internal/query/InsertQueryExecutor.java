/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.query;

import com.google.common.collect.ImmutableMap;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.Data;
import io.mindmaps.core.model.*;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.internal.GraqlType;
import io.mindmaps.graql.admin.VarAdmin;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.mindmaps.constants.ErrorMessage.INSERT_NON_RESOURCE_WITH_VALUE;

/**
 * A class for executing insert queries.
 *
 * This behaviour is moved to its own class to allow InsertQueryImpl to have fewer mutable fields.
 */
class InsertQueryExecutor {

    private final MindmapsGraph graph;
    private final Collection<VarAdmin> vars;
    private final Map<String, Concept> concepts = new HashMap<>();
    private final Stack<String> visitedVars = new Stack<>();
    private final ImmutableMap<String, List<VarAdmin>> varsByName;
    private final ImmutableMap<String, List<VarAdmin>> varsById;

    InsertQueryExecutor(Collection<VarAdmin> vars, MindmapsGraph graph) {
        this.vars = vars;
        this.graph = graph;

        // Group variables by name
        varsByName = ImmutableMap.copyOf(
                vars.stream().collect(Collectors.groupingBy(VarAdmin::getName))
        );

        // Group variables by id (if they have one defined)
        // the 'filter' step guarantees the remaining have an ID
        //noinspection OptionalGetWithoutIsPresent
        varsById = ImmutableMap.copyOf(
                vars.stream()
                        .filter(var -> var.getId().isPresent())
                        .collect(Collectors.groupingBy(var -> var.getId().get()))
        );
    }

    /**
     * Insert all the Vars
     */
    Stream<Concept> insertAll() {
        return insertAll(new HashMap<>());
    }

    /**
     * Insert all the Vars
     * @param results the result of a match query
     */
    Stream<Concept> insertAll(Map<String, Concept> results) {
        concepts.clear();
        concepts.putAll(new HashMap<>(results));

        // First insert each var
        vars.forEach(this::insertVar);

        // Then add resources to each var, streaming out the results.
        // It is necessary to add resources last to be sure that any 'has-resource' information in the query has
        // already been added.
        // TODO: Fix this so this step is no longer necessary (can be done by correctly expanding 'has-resource')
        return vars.stream().map(this::insertVarResources);
    }

    /**
     * @param var the Var to insert into the graph
     */
    private Concept insertVar(VarAdmin var) {
        Concept concept = getConcept(var);

        if (var.getAbstract()) concept.asType().setAbstract(true);

        var.getHasRoles().forEach(role -> concept.asRelationType().hasRole(getConcept(role).asRoleType()));
        var.getPlaysRoles().forEach(role -> concept.asType().playsRole(getConcept(role).asRoleType()));
        var.getScopes().forEach(scope -> concept.asRelation().scope(getConcept(scope).asInstance()));

        var.getHasResourceTypes().forEach(resourceType -> addResourceType(var, resourceType));

        var.getCastings().forEach(casting -> addCasting(var, casting));

        return concept;
    }

    /**
     * Add all the resources of the given var
     * @param var the var to add resources to
     */
    private Concept insertVarResources(VarAdmin var) {
        Concept concept = getConcept(var);

        if (!var.getResourceEqualsPredicates().isEmpty()) {
            Instance instance = concept.asInstance();
            var.getResourceEqualsPredicates().forEach((type, values) -> addResources(instance, type, values));
        }

        return concept;
    }

    /**
     * @param var the Var that is represented by a concept in the graph
     * @return the same as addConcept, but using an internal map to remember previous calls
     */
    private Concept getConcept(VarAdmin var) {
        String name = var.getName();
        if (visitedVars.contains(name)) {
            throw new IllegalStateException(ErrorMessage.INSERT_RECURSIVE.getMessage(var.getPrintableName()));
        }

        visitedVars.push(name);
        Concept concept = concepts.computeIfAbsent(name, n -> addConcept(var));
        visitedVars.pop();
        return concept;
    }

    /**
     * @param var the Var that is to be added into the graph
     * @return the concept representing the given Var, creating it if it doesn't exist
     */
    private Concept addConcept(VarAdmin var) {
        var = mergeVar(var);

        Optional<VarAdmin> type = var.getType();
        Optional<VarAdmin> ako = var.getAko();

        if (type.isPresent() && ako.isPresent()) {
            String printableName = var.getPrintableName();
            throw new IllegalStateException(ErrorMessage.INSERT_ISA_AND_AKO.getMessage(printableName));
        }

        Concept concept;

        // If 'ako' provided, use that, else use 'isa', else get existing concept by id
        if (ako.isPresent()) {
            String id = getTypeIdOrThrow(var.getId());
            concept = putConceptBySuperType(id, getConcept(ako.get()).asType());
        } else if (type.isPresent()) {
            concept = putConceptByType(var.getId(), var, getConcept(type.get()).asType());
        } else {
            concept = var.getId().map(graph::getConcept).orElse(null);
        }

        if (concept == null) {
            System.out.println(varsById);
            throw new IllegalStateException(
                    var.getId().map(ErrorMessage.INSERT_GET_NON_EXISTENT_ID::getMessage)
                            .orElse(ErrorMessage.INSERT_UNDEFINED_VARIABLE.getMessage(var.getName()))
            );
        }

        return concept;
    }

    /**
     * Merge a variable with any other variables referred to with the same variable name or id
     * @param var the variable to merge
     * @return the merged variable
     */
    private VarAdmin mergeVar(VarAdmin var) {
        boolean changed = true;
        Set<VarAdmin> varsToMerge = new HashSet<>();

        // Keep merging until the set of merged variables stops changing
        // This handles cases when variables are referred to with multiple degrees of separation
        // e.g.
        // "123" isa movie; $x id "123"; $y id "123"; ($y, $z)
        while (changed) {
            // Merge variable referred to by name...
            boolean byNameChange = varsToMerge.addAll(varsByName.get(var.getName()));
            var = new VarImpl(varsToMerge);

            // Then merge variables referred to by id...
            boolean byIdChange = var.getId().map(id -> varsToMerge.addAll(varsById.get(id))).orElse(false);
            var = new VarImpl(varsToMerge);

            changed = byNameChange | byIdChange;
        }

        return var;
    }

    /**
     * @param id the ID of the concept
     * @param var the Var representing the concept in the insert query
     * @param type the type of the concept
     * @return a concept with the given ID and the specified type
     */
    private Concept putConceptByType(Optional<String> id, VarAdmin var, Type type) {
        String typeId = type.getId();

        if (!type.isResourceType() && !var.getValuePredicates().isEmpty()) {
            throw new IllegalStateException(INSERT_NON_RESOURCE_WITH_VALUE.getMessage(type.getId()));
        }

        if (typeId.equals(DataType.ConceptMeta.ENTITY_TYPE.getId())) {
            return graph.putEntityType(getTypeIdOrThrow(id));
        } else if (typeId.equals(DataType.ConceptMeta.RELATION_TYPE.getId())) {
            return graph.putRelationType(getTypeIdOrThrow(id));
        } else if (typeId.equals(DataType.ConceptMeta.ROLE_TYPE.getId())) {
            return graph.putRoleType(getTypeIdOrThrow(id));
        } else if (typeId.equals(DataType.ConceptMeta.RESOURCE_TYPE.getId())) {
            return graph.putResourceType(getTypeIdOrThrow(id), getDataType(var));
        } else if (typeId.equals(DataType.ConceptMeta.RULE_TYPE.getId())) {
            return graph.putRuleType(getTypeIdOrThrow(id));
        } else if (type.isEntityType()) {
            return putInstance(id, type.asEntityType(), graph::putEntity, graph::addEntity);
        } else if (type.isRelationType()) {
            return putInstance(id, type.asRelationType(), graph::putRelation, graph::addRelation);
        } else if (type.isResourceType()) {
            return graph.putResource(getValue(var), type.asResourceType());
        } else if (type.isRuleType()) {
            String lhs = var.getLhs().get();
            String rhs = var.getRhs().get();

            return putInstance(
                    id, type.asRuleType(),
                    (ruleId, ruleType) -> graph.putRule(ruleId, lhs, rhs, ruleType),
                    ruleType -> graph.addRule(lhs, rhs, ruleType)
            );
        } else {
            throw new RuntimeException("Unrecognized type " + type.getId());
        }
    }

    /**
     * @param id the ID of the concept
     * @param superType the supertype of the concept
     * @return a concept with the given ID and the specified supertype
     */
    private <T> Concept putConceptBySuperType(String id, Type superType) {
        if (superType.isEntityType()) {
            return graph.putEntityType(id).superType(superType.asEntityType());
        } else if (superType.isRelationType()) {
            return graph.putRelationType(id).superType(superType.asRelationType());
        } else if (superType.isRoleType()) {
            return graph.putRoleType(id).superType(superType.asRoleType());
        } else if (superType.isResourceType()) {
            ResourceType<T> superResource = superType.asResourceType();
            return graph.putResourceType(id, superResource.getDataType()).superType(superResource);
        } else if (superType.isRuleType()) {
            return graph.putRuleType(id).superType(superType.asRuleType());
        } else {
            throw new IllegalStateException(ErrorMessage.INSERT_METATYPE.getMessage(id, superType.getId()));
        }
    }

    /**
     * Put an instance of a type which may or may not have an ID specified
     * @param id the ID of the instance to create, or empty to not specify an ID
     * @param type the type of the instance
     * @param putInstance a 'put' method on a MindmapsGraph, such as graph::putEntity
     * @param addInstance an 'add' method on a MindmapsGraph such a graph::addEntity
     * @param <T> the class of the type of the instance, e.g. EntityType
     * @param <S> the class of the instance, e.g. Entity
     * @return an instance of the specified type, with the given ID if one was specified
     */
    private <T extends Type, S extends Instance> S putInstance(
            Optional<String> id, T type, BiFunction<String, T, S> putInstance, Function<T, S> addInstance
    ) {
        return id.map(i -> putInstance.apply(i, type)).orElseGet(() -> addInstance.apply(type));
    }

    /**
     * Get an ID from an optional for a type, throwing an exception if it is not present.
     * This is because types must have specified IDs.
     * @param id an optional ID to get
     * @return the ID, if present
     * @throws IllegalStateException if the ID was not present
     */
    private String getTypeIdOrThrow(Optional<String> id) throws IllegalStateException {
        return id.orElseThrow(() -> new IllegalStateException(ErrorMessage.INSERT_TYPE_WITHOUT_ID.getMessage()));
    }

    /**
     * Add a roleplayer to the given relation
     * @param var the variable representing the relation
     * @param casting a casting between a role type and role player
     */
    private void addCasting(VarAdmin var, Var.Casting casting) {
        Relation relation = getConcept(var).asRelation();

        VarAdmin roleVar = casting.getRoleType().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.INSERT_RELATION_WITHOUT_ROLE_TYPE.getMessage())
        );

        RoleType roleType = getConcept(roleVar).asRoleType();
        Instance roleplayer = getConcept(casting.getRolePlayer()).asInstance();
        relation.putRolePlayer(roleType, roleplayer);
    }

    private Object getValue(VarAdmin var) {
        Iterator<?> values = var.getValueEqualsPredicates().iterator();

        if (values.hasNext()) {
            Object value = values.next();

            if (values.hasNext()) {
                throw new IllegalStateException(ErrorMessage.INSERT_MULTIPLE_VALUES.getMessage(value, values.next()));
            }

            return value;
        } else {
            throw new IllegalStateException(ErrorMessage.INSERT_RESOURCE_WITHOUT_VALUE.getMessage());
        }
    }

    /**
     * Get the datatype of a Var if specified, else throws an IllegalStateException
     * @return the datatype of the given var
     */
    private Data<?> getDataType(VarAdmin var) {
        return var.getDatatype().orElseThrow(
                () -> new IllegalStateException(ErrorMessage.INSERT_NO_DATATYPE.getMessage(var.getPrintableName()))
        );
    }

    /**
     * @param var the var representing a type that can own the given resource type
     * @param resourceVar the var representing the resource type
     */
    private void addResourceType(VarAdmin var, VarAdmin resourceVar) {
        Type type = getConcept(var).asType();
        ResourceType resourceType = getConcept(resourceVar).asResourceType();

        RoleType owner = graph.putRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(resourceType.getId()));
        RoleType value = graph.putRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(resourceType.getId()));
        graph.putRelationType(GraqlType.HAS_RESOURCE.getId(resourceType.getId()))
                .hasRole(owner).hasRole(value);

        type.playsRole(owner);
        resourceType.playsRole(value);
    }

    /**
     * Add resources to the given instance, using the has-resource relation
     * @param instance the instance to add resources to
     * @param resourceType the variable representing the resource type
     * @param values a set of values to set on the resource instances
     * @param <D> the type of the resources
     */
    private <D> void addResources(Instance instance, VarAdmin resourceType, Set<D> values) {
        // We assume the resource type has the correct datatype. If it does not, core will catch the problem
        //noinspection unchecked
        ResourceType<D> type = getConcept(resourceType).asResourceType();
        values.forEach(value -> addResource(instance, type, value));
    }

    /**
     * Add a resource to the given instance, using the has-resource relation
     * @param instance the instance to add a resource to
     * @param type the resource type
     * @param value the value to set on the resource instance
     * @param <D> the type of the resource
     */
    private <D> void addResource(Instance instance, ResourceType<D> type, D value) {
        Resource resource = graph.putResource(value, type);

        RelationType hasResource = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(type.getId()));
        RoleType hasResourceTarget = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(type.getId()));
        RoleType hasResourceValue = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(type.getId()));

        if (hasResource == null || hasResourceTarget == null || hasResourceValue == null) {
            throw new IllegalStateException(
                    ErrorMessage.INSERT_NO_RESOURCE_RELATION.getMessage(instance.type().getId(), type.getId())
            );
        }

        Relation relation = graph.addRelation(hasResource);
        relation.putRolePlayer(hasResourceTarget, instance);
        relation.putRolePlayer(hasResourceValue, resource);
    }
}
