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

package ai.grakn.graql.internal.query;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.LhsProperty;
import ai.grakn.graql.internal.pattern.property.RhsProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.util.CommonUtil.optionalOr;
import static ai.grakn.util.ErrorMessage.INSERT_INSTANCE_WITH_NAME;
import static ai.grakn.util.ErrorMessage.INSERT_NON_RESOURCE_WITH_VALUE;

/**
 * A class for executing insert queries.
 *
 * This behaviour is moved to its own class to allow InsertQueryImpl to have fewer mutable fields.
 */
public class InsertQueryExecutor {

    private final GraknGraph graph;
    private final Collection<VarAdmin> vars;
    private final Map<String, Concept> concepts = new HashMap<>();
    private final Stack<String> visitedVars = new Stack<>();
    private final ImmutableMap<String, List<VarAdmin>> varsByVarName;
    private final ImmutableMap<String, List<VarAdmin>> varsByTypeName;
    private final ImmutableMap<String, List<VarAdmin>> varsById;

    InsertQueryExecutor(Collection<VarAdmin> vars, GraknGraph graph) {
        this.vars = vars;
        this.graph = graph;

        // Group variables by variable name
        varsByVarName = ImmutableMap.copyOf(
                vars.stream().collect(Collectors.groupingBy(VarAdmin::getVarName))
        );

        // Group variables by id (if they have one defined)
        // the 'filter' step guarantees the remaining have an ID
        //noinspection OptionalGetWithoutIsPresent
        varsById = ImmutableMap.copyOf(
                vars.stream()
                        .filter(var -> var.getId().isPresent())
                        .collect(Collectors.groupingBy(var -> var.getId().get()))
        );

        // Group variables by type name (if they have one defined)
        // the 'filter' step guarantees the remaining have a name
        //noinspection OptionalGetWithoutIsPresent
        varsByTypeName = ImmutableMap.copyOf(
                vars.stream()
                        .filter(var -> var.getName().isPresent())
                        .collect(Collectors.groupingBy(var -> var.getName().get()))
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
        return vars.stream().map(this::insertVar);
    }

    /**
     * @param var the Var to insert into the graph
     */
    private Concept insertVar(VarAdmin var) {
        Concept concept = getConcept(var);
        var.getProperties().forEach(property -> ((VarPropertyInternal) property).insert(this, concept));
        return concept;
    }

    public GraknGraph getGraph() {
        return graph;
    }

    /**
     * @param var the Var that is represented by a concept in the graph
     * @return the same as addConcept, but using an internal map to remember previous calls
     */
    public Concept getConcept(VarAdmin var) {
        String name = var.getVarName();
        if (visitedVars.contains(name)) {
            throw new IllegalStateException(ErrorMessage.INSERT_RECURSIVE.getMessage(var.getPrintableName()));
        }

        visitedVars.push(name);
        Concept concept = concepts.computeIfAbsent(name, n -> addConcept(var));
        visitedVars.pop();
        return concept;
    }

    /**
     * @param varToAdd the Var that is to be added into the graph
     * @return the concept representing the given Var, creating it if it doesn't exist
     */
    private Concept addConcept(VarAdmin varToAdd) {
        VarAdmin var = mergeVar(varToAdd);

        Optional<VarAdmin> typeVar = var.getProperty(IsaProperty.class).map(IsaProperty::getType);
        Optional<VarAdmin> subVar = getSub(var);

        if (typeVar.isPresent() && subVar.isPresent()) {
            String printableName = var.getPrintableName();
            throw new IllegalStateException(ErrorMessage.INSERT_ISA_AND_SUB.getMessage(printableName));
        }

        // Use either sub or isa to decide type
        Optional<Type> typeConcept = optionalOr(
                subVar.map(this::getConcept).map(Concept::type),
                typeVar.map(this::getConcept).map(Concept::asType)
        );

        // If type provided, then 'put' the concept, else 'get' it by ID or name
        Optional<Concept> concept = optionalOr(
                typeConcept.map(type -> putConceptByType(var.getName(), var.getId(), var, type)),
                var.getId().map(graph::<Concept>getConcept),
                var.getName().map(graph::getType)
        );

        if (concept.isPresent()) {
            return concept.get();
        } else {
            String message;

            if (subVar.isPresent()) {
                String subId = subVar.get().getName().orElse("<no-name>");
                message = ErrorMessage.INSERT_METATYPE.getMessage(var.getPrintableName(), subId);
            } else {
                message = var.getId().map(ErrorMessage.INSERT_WITHOUT_TYPE::getMessage)
                                .orElse(ErrorMessage.INSERT_UNDEFINED_VARIABLE.getMessage(var.getPrintableName()));
            }

            throw new IllegalStateException(message);
        }
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
        // id "123" isa movie; $x id "123", name "Bob"; $y id "123"; ($y, $z)
        while (changed) {
            // Merge variable referred to by variable name...
            List<VarAdmin> vars = varsByVarName.getOrDefault(var.getVarName(), Lists.newArrayList());
            vars.add(var);
            boolean byVarNameChange = varsToMerge.addAll(vars);
            var = Patterns.mergeVars(varsToMerge);

            // Then merge variables referred to by id...
            boolean byIdChange = var.getId().map(id -> varsToMerge.addAll(varsById.get(id))).orElse(false);
            var = Patterns.mergeVars(varsToMerge);

            // And finally merge variables referred to by type name...
            boolean byTypeNameChange = var.getName().map(id -> varsToMerge.addAll(varsByTypeName.get(id))).orElse(false);
            var = Patterns.mergeVars(varsToMerge);

            changed = byVarNameChange | byIdChange | byTypeNameChange;
        }

        return var;
    }

    /**
     * @param name the name of the concept
     * @param id the ID of the concept
     * @param var the Var representing the concept in the insert query
     * @param type the type of the concept
     * @return a concept with the given ID and the specified type
     */
    private Concept putConceptByType(Optional<String> name, Optional<String> id, VarAdmin var, Type type) {
        String typeName = type.getName();

        if (!type.isResourceType() && var.hasProperty(ValueProperty.class)) {
            throw new IllegalStateException(INSERT_NON_RESOURCE_WITH_VALUE.getMessage(type.getId()));
        }

        if (typeName.equals(Schema.MetaSchema.ENTITY_TYPE.getId())) {
            return graph.putEntityType(getTypeNameOrThrow(name));
        } else if (typeName.equals(Schema.MetaSchema.RELATION_TYPE.getId())) {
            return graph.putRelationType(getTypeNameOrThrow(name));
        } else if (typeName.equals(Schema.MetaSchema.ROLE_TYPE.getId())) {
            return graph.putRoleType(getTypeNameOrThrow(name));
        } else if (typeName.equals(Schema.MetaSchema.RESOURCE_TYPE.getId())) {
            return graph.putResourceType(getTypeNameOrThrow(name), getDataType(var));
        } else if (typeName.equals(Schema.MetaSchema.RULE_TYPE.getId())) {
            return graph.putRuleType(getTypeNameOrThrow(name));
        }

        // Concept must be an instance, so confirm it has no name set
        name.ifPresent(theName -> {
            throw new IllegalStateException(INSERT_INSTANCE_WITH_NAME.getMessage(theName));
        });

        if (type.isEntityType()) {
            return addOrGetInstance(id, type.asEntityType()::addEntity);
        } else if (type.isRelationType()) {
            return addOrGetInstance(id, type.asRelationType()::addRelation);
        } else if (type.isResourceType()) {
            return addOrGetInstance(id,
                    () -> type.asResourceType().putResource(getValue(var))
            );
        } else if (type.isRuleType()) {
            return addOrGetInstance(id, () -> {
                Pattern lhs = var.getProperty(LhsProperty.class).get().getLhs();
                Pattern rhs = var.getProperty(RhsProperty.class).get().getRhs();
                return type.asRuleType().addRule(lhs, rhs);
            });
        } else {
            throw new RuntimeException("Unrecognized type " + type.getId());
        }
    }

    /**
     * Put an instance of a type which may or may not have an ID specified
     * @param id the ID of the instance to create, or empty to not specify an ID
     * @param addInstance an 'add' method on a GraknGraph such a graph::addEntity
     * @param <T> the class of the type of the instance, e.g. EntityType
     * @param <S> the class of the instance, e.g. Entity
     * @return an instance of the specified type, with the given ID if one was specified
     */
    private <T extends Type, S extends Instance> S addOrGetInstance(
            Optional<String> id, Supplier<S> addInstance
    ) {
        return id.map(graph::<S>getConcept).orElseGet(addInstance);
    }

    /**
     * Get a name from an optional for a type, throwing an exception if it is not present.
     * This is because types must have specified names.
     * @param name an optional name to get
     * @return the name, if present
     * @throws IllegalStateException if the name was not present
     */
    private String getTypeNameOrThrow(Optional<String> name) throws IllegalStateException {
        return name.orElseThrow(() -> new IllegalStateException(ErrorMessage.INSERT_TYPE_WITHOUT_NAME.getMessage()));
    }

    private Object getValue(VarAdmin var) {
        Iterator<ValueProperty> properties = var.getProperties(ValueProperty.class).iterator();

        if (properties.hasNext()) {
            // Value properties are confirmed to be "equals" only in the ValueProperty class
            //noinspection OptionalGetWithoutIsPresent
            Object value = properties.next().getPredicate().equalsValue().get();

            if (properties.hasNext()) {
                throw new IllegalStateException(ErrorMessage.INSERT_MULTIPLE_VALUES.getMessage(
                        value, properties.next().getPredicate())
                );
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
    private ResourceType.DataType<?> getDataType(VarAdmin var) {
        Optional<ResourceType.DataType<?>> directDataType =
                var.getProperty(DataTypeProperty.class).map(DataTypeProperty::getDatatype);

        Optional<ResourceType.DataType<?>> indirectDataType =
                getSub(var).map(sub -> getConcept(sub).asResourceType().getDataType());

        Optional<ResourceType.DataType<?>> dataType = optionalOr(directDataType, indirectDataType);

        return dataType.orElseThrow(
                () -> new IllegalStateException(ErrorMessage.INSERT_NO_DATATYPE.getMessage(var.getPrintableName()))
        );
    }

    private Optional<VarAdmin> getSub(VarAdmin var) {
        return var.getProperty(SubProperty.class).map(SubProperty::getSuperType);
    }
}
