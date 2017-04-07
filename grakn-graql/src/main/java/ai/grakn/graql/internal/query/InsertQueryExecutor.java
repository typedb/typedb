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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
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

import static ai.grakn.graql.internal.util.CommonUtil.optionalOr;
import static ai.grakn.util.ErrorMessage.INSERT_INSTANCE_WITH_NAME;
import static ai.grakn.util.ErrorMessage.INSERT_ISA_AND_SUB;
import static ai.grakn.util.ErrorMessage.INSERT_MULTIPLE_VALUES;
import static ai.grakn.util.ErrorMessage.INSERT_NO_DATATYPE;
import static ai.grakn.util.ErrorMessage.INSERT_RECURSIVE;
import static ai.grakn.util.ErrorMessage.INSERT_RESOURCE_WITHOUT_VALUE;
import static ai.grakn.util.ErrorMessage.INSERT_RULE_WITHOUT_LHS;
import static ai.grakn.util.ErrorMessage.INSERT_RULE_WITHOUT_RHS;
import static ai.grakn.util.ErrorMessage.INSERT_TYPE_WITHOUT_LABEL;
import static ai.grakn.util.ErrorMessage.INSERT_UNDEFINED_VARIABLE;
import static ai.grakn.util.ErrorMessage.INSERT_WITHOUT_TYPE;
import static ai.grakn.util.ErrorMessage.LABEL_NOT_FOUND;

/**
 * A class for executing insert queries.
 *
 * This behaviour is moved to its own class to allow InsertQueryImpl to have fewer mutable fields.
 *
 * @author Felix Chapman
 */
public class InsertQueryExecutor {

    private final GraknGraph graph;
    private final Collection<VarAdmin> vars;
    private final Map<VarName, Concept> concepts = new HashMap<>();
    private final Map<VarName, Concept> namedConcepts = new HashMap<>();
    private final Stack<VarName> visitedVars = new Stack<>();
    private final ImmutableMap<VarName, List<VarAdmin>> varsByVarName;
    private final ImmutableMap<TypeLabel, List<VarAdmin>> varsByTypeLabel;
    private final ImmutableMap<ConceptId, List<VarAdmin>> varsById;

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
        varsByTypeLabel = ImmutableMap.copyOf(
                vars.stream()
                        .filter(var -> var.getTypeLabel().isPresent())
                        .collect(Collectors.groupingBy(var -> var.getTypeLabel().get()))
        );
    }

    /**
     * Insert all the Vars
     */
    Map<VarName, Concept> insertAll() {
        return insertAll(new HashMap<>());
    }

    /**
     * Insert all the Vars
     * @param results the result of a match query
     */
    Map<VarName, Concept> insertAll(Map<VarName, Concept> results) {
        concepts.clear();
        concepts.putAll(results);
        namedConcepts.clear();
        namedConcepts.putAll(results);
        vars.forEach(this::insertVar);
        return namedConcepts;
    }

    /**
     * @param var the Var to insert into the graph
     */
    private void insertVar(VarAdmin var) {
        Concept concept = getConcept(var);
        var.getProperties().forEach(property -> ((VarPropertyInternal) property).insert(this, concept));
    }

    public GraknGraph getGraph() {
        return graph;
    }

    /**
     * @param var the Var that is represented by a concept in the graph
     * @return the same as addConcept, but using an internal map to remember previous calls
     */
    public Concept getConcept(VarAdmin var) {
        VarName name = var.getVarName();
        if (visitedVars.contains(name)) {
            throw new IllegalStateException(INSERT_RECURSIVE.getMessage(var.getPrintableName()));
        }

        visitedVars.push(name);
        Concept concept = concepts.computeIfAbsent(name, n -> addConcept(var));
        assert concept != null : var ;
        if (var.isUserDefinedName()) namedConcepts.put(name, concept);
        visitedVars.pop();
        return concept;
    }

    /**
     * @param varToAdd the Var that is to be added into the graph
     * @return the concept representing the given Var, creating it if it doesn't exist
     */
    private Concept addConcept(VarAdmin varToAdd) {
        VarAdmin var = mergeVar(varToAdd);

        Optional<IsaProperty> type = var.getProperty(IsaProperty.class);
        Optional<SubProperty> sub = var.getProperty(SubProperty.class);

        if (type.isPresent() && sub.isPresent()) {
            String printableName = var.getPrintableName();
            throw new IllegalStateException(INSERT_ISA_AND_SUB.getMessage(printableName));
        }

        Optional<TypeLabel> typeLabel = var.getTypeLabel();
        Optional<ConceptId> id = var.getId();

        typeLabel.ifPresent(label -> {
            if (type.isPresent()) {
                throw new IllegalStateException(INSERT_INSTANCE_WITH_NAME.getMessage(label));
            }
        });

        // If type provided, then 'put' the concept, else 'get' it by ID or label
        if (sub.isPresent()) {
            TypeLabel label = getTypeLabelOrThrow(typeLabel);
            return putType(label, var, sub.get());
        } else if (type.isPresent()) {
            return putInstance(id, var, type.get());
        } else if (id.isPresent()) {
            Concept concept = graph.getConcept(id.get());
            if (concept == null) throw new IllegalStateException(INSERT_WITHOUT_TYPE.getMessage(id.get()));
            return concept;
        } else if (typeLabel.isPresent()) {
            Concept concept = graph.getType(typeLabel.get());
            if (concept == null) throw new IllegalStateException(LABEL_NOT_FOUND.getMessage(typeLabel.get()));
            return concept;
        } else {
            throw new IllegalStateException(INSERT_UNDEFINED_VARIABLE.getMessage(var.getPrintableName()));
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
            boolean byTypeLabelChange = var.getTypeLabel().map(id -> varsToMerge.addAll(varsByTypeLabel.get(id))).orElse(false);
            var = Patterns.mergeVars(varsToMerge);

            changed = byVarNameChange | byIdChange | byTypeLabelChange;
        }

        return var;
    }

    /**
     * @param id the ID of the concept
     * @param var the Var representing the concept in the insert query
     * @param isa the type property of the var
     * @return a concept with the given ID and the specified type
     */
    private Instance putInstance(Optional<ConceptId> id, VarAdmin var, IsaProperty isa) {
        Type type = getConcept(isa.getType()).asType();

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
                LhsProperty lhs = var.getProperty(LhsProperty.class)
                        .orElseThrow(() -> new IllegalStateException(INSERT_RULE_WITHOUT_LHS.getMessage(var)));
                RhsProperty rhs = var.getProperty(RhsProperty.class)
                        .orElseThrow(() -> new IllegalStateException(INSERT_RULE_WITHOUT_RHS.getMessage(var)));
                return type.asRuleType().putRule(lhs.getPattern(), rhs.getPattern());
            });
        } else if (type.getLabel().equals(Schema.MetaSchema.CONCEPT.getLabel())) {
            throw new IllegalStateException(var + " cannot be an instance of meta-type " + type.getLabel());
        } else {
            throw new RuntimeException("Unrecognized type " + type.getLabel());
        }
    }

    /**
     * @param label the label of the concept
     * @param var the Var representing the concept in the insert query
     * @param sub the supertype property of the var
     * @return a concept with the given ID and the specified type
     */
    private Type putType(TypeLabel label, VarAdmin var, SubProperty sub) {
        Type superType = getConcept(sub.getSuperType()).asType();

        if (superType.isEntityType()) {
            return graph.putEntityType(label).superType(superType.asEntityType());
        } else if (superType.isRelationType()) {
            return graph.putRelationType(label).superType(superType.asRelationType());
        } else if (superType.isRoleType()) {
            return graph.putRoleType(label).superType(superType.asRoleType());
        } else if (superType.isResourceType()) {
            return graph.putResourceType(label, getDataType(var)).superType(superType.asResourceType());
        } else if (superType.isRuleType()) {
            return graph.putRuleType(label).superType(superType.asRuleType());
        } else {
            throw new IllegalStateException(ErrorMessage.INSERT_METATYPE.getMessage(label, superType.getLabel()));
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
    private <T extends Type, S extends Instance> S addOrGetInstance(Optional<ConceptId> id, Supplier<S> addInstance) {
        return id.map(graph::<S>getConcept).orElseGet(addInstance);
    }

    /**
     * Get a label from an optional for a type, throwing an exception if it is not present.
     * This is because types must have specified labels.
     * @param label an optional label to get
     * @return the label, if present
     * @throws IllegalStateException if the label was not present
     */
    private TypeLabel getTypeLabelOrThrow(Optional<TypeLabel> label) throws IllegalStateException {
        return label.orElseThrow(() -> new IllegalStateException(INSERT_TYPE_WITHOUT_LABEL.getMessage()));
    }

    private Object getValue(VarAdmin var) {
        Iterator<ValueProperty> properties = var.getProperties(ValueProperty.class).iterator();

        if (properties.hasNext()) {
            // Value properties are confirmed to be "equals" only in the ValueProperty class
            //noinspection OptionalGetWithoutIsPresent
            Object value = properties.next().getPredicate().equalsValue().get();

            if (properties.hasNext()) {
                throw new IllegalStateException(INSERT_MULTIPLE_VALUES.getMessage(
                        value, properties.next().getPredicate())
                );
            }

            return value;
        } else {
            throw new IllegalStateException(INSERT_RESOURCE_WITHOUT_VALUE.getMessage());
        }
    }

    /**
     * Get the datatype of a Var if specified, else throws an IllegalStateException
     * @return the datatype of the given var
     */
    private ResourceType.DataType<?> getDataType(VarAdmin var) {
        Optional<ResourceType.DataType<?>> directDataType =
                var.getProperty(DataTypeProperty.class).map(DataTypeProperty::getDataType);

        Optional<ResourceType.DataType<?>> indirectDataType =
                getSub(var).map(sub -> getConcept(sub).asResourceType().getDataType());

        Optional<ResourceType.DataType<?>> dataType = optionalOr(directDataType, indirectDataType);

        return dataType.orElseThrow(
                () -> new IllegalStateException(INSERT_NO_DATATYPE.getMessage(var.getPrintableName()))
        );
    }

    private Optional<VarAdmin> getSub(VarAdmin var) {
        return var.getProperty(SubProperty.class).map(SubProperty::getSuperType);
    }
}
