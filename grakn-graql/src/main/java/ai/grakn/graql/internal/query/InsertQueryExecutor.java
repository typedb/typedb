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
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.WhenProperty;
import ai.grakn.graql.internal.pattern.property.ThenProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.CommonUtil;
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

import static ai.grakn.util.CommonUtil.optionalOr;

/**
 * A class for executing insert queries.
 *
 * This behaviour is moved to its own class to allow InsertQueryImpl to have fewer mutable fields.
 *
 * @author Felix Chapman
 */
public class InsertQueryExecutor {

    private final GraknGraph graph;
    private final Collection<VarPatternAdmin> vars;
    private final Map<Var, Concept> concepts = new HashMap<>();
    private final Map<Var, Concept> namedConcepts = new HashMap<>();
    private final Stack<Var> visitedVars = new Stack<>();
    private final ImmutableMap<Var, List<VarPatternAdmin>> varsByVarName;
    private final ImmutableMap<Label, List<VarPatternAdmin>> varsByTypeLabel;
    private final ImmutableMap<ConceptId, List<VarPatternAdmin>> varsById;

    InsertQueryExecutor(Collection<VarPatternAdmin> vars, GraknGraph graph) {
        this.vars = vars;
        this.graph = graph;

        // Group variables by variable name
        varsByVarName = ImmutableMap.copyOf(
                vars.stream().collect(Collectors.groupingBy(VarPatternAdmin::getVarName))
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
    Answer insertAll() {
        return insertAll(new QueryAnswer());
    }

    /**
     * Insert all the Vars
     * @param results the result of a match query
     */
    Answer insertAll(Answer results) {
        concepts.clear();
        concepts.putAll(results.map());
        namedConcepts.clear();
        namedConcepts.putAll(results.map());
        vars.forEach(this::insertVar);
        return new QueryAnswer(namedConcepts);
    }

    /**
     * @param var the {@link VarPatternAdmin} to insert into the graph
     */
    private void insertVar(VarPatternAdmin var) {
        Concept concept = getConcept(var);
        var.getProperties().forEach(property -> ((VarPropertyInternal) property).insert(this, concept));
    }

    public GraknGraph getGraph() {
        return graph;
    }

    /**
     * @param var the {@link VarPatternAdmin} that is represented by a concept in the graph
     * @return the same as addConcept, but using an internal map to remember previous calls
     */
    public Concept getConcept(VarPatternAdmin var) {
        Var name = var.getVarName();
        if (visitedVars.contains(name)) {
            throw GraqlQueryException.insertRecursive(var);
        }

        visitedVars.push(name);
        Concept concept = concepts.computeIfAbsent(name, n -> addConcept(var));
        assert concept != null : var ;
        if (var.getVarName().isUserDefinedName()) namedConcepts.put(name, concept);
        visitedVars.pop();
        return concept;
    }

    /**
     * @param varToAdd the {@link VarPatternAdmin} that is to be added into the graph
     * @return the concept representing the given {@link VarPatternAdmin}, creating it if it doesn't exist
     */
    private Concept addConcept(VarPatternAdmin varToAdd) {
        VarPatternAdmin var = mergeVar(varToAdd);

        Optional<IsaProperty> type = var.getProperty(IsaProperty.class);
        Optional<SubProperty> sub = var.getProperty(SubProperty.class);

        if (type.isPresent() && sub.isPresent()) {
            String printableName = var.getPrintableName();
            throw GraqlQueryException.insertIsaAndSub(printableName);
        }

        Optional<Label> typeLabel = var.getTypeLabel();
        Optional<ConceptId> id = var.getId();

        typeLabel.ifPresent(label -> {
            if (type.isPresent()) {
                throw GraqlQueryException.insertInstanceWithLabel(label);
            }
        });

        // If type provided, then 'put' the concept, else 'get' it by ID or label
        if (sub.isPresent()) {
            Label label = getTypeLabelOrThrow(typeLabel);
            return putOntologyConcept(label, var, sub.get());
        } else if (type.isPresent()) {
            return putInstance(id, var, type.get());
        } else if (id.isPresent()) {
            Concept concept = graph.getConcept(id.get());
            if (concept == null) throw GraqlQueryException.insertWithoutType(id.get());
            return concept;
        } else if (typeLabel.isPresent()) {
            Concept concept = graph.getOntologyConcept(typeLabel.get());
            if (concept == null) throw GraqlQueryException.labelNotFound(typeLabel.get());
            return concept;
        } else {
            throw GraqlQueryException.insertUndefinedVariable(var);
        }
    }

    /**
     * Merge a variable with any other variables referred to with the same variable name or id
     * @param var the variable to merge
     * @return the merged variable
     */
    private VarPatternAdmin mergeVar(VarPatternAdmin var) {
        boolean changed = true;
        Set<VarPatternAdmin> varsToMerge = new HashSet<>();

        // Keep merging until the set of merged variables stops changing
        // This handles cases when variables are referred to with multiple degrees of separation
        // e.g.
        // id "123" isa movie; $x id "123", name "Bob"; $y id "123"; ($y, $z)
        while (changed) {
            // Merge variable referred to by variable name...
            List<VarPatternAdmin> vars = varsByVarName.getOrDefault(var.getVarName(), Lists.newArrayList());
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
     * @param var the {@link VarPatternAdmin} representing the concept in the insert query
     * @param isa the type property of the var
     * @return a concept with the given ID and the specified type
     */
    private Thing putInstance(Optional<ConceptId> id, VarPatternAdmin var, IsaProperty isa) {
        Type type = getConcept(isa.type()).asType();

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
                WhenProperty when = var.getProperty(WhenProperty.class)
                        .orElseThrow(() -> GraqlQueryException.insertRuleWithoutLhs(var));
                ThenProperty then = var.getProperty(ThenProperty.class)
                        .orElseThrow(() -> GraqlQueryException.insertRuleWithoutRhs(var));
                return type.asRuleType().putRule(when.getPattern(), then.getPattern());
            });
        } else if (type.getLabel().equals(Schema.MetaSchema.THING.getLabel())) {
            throw GraqlQueryException.createInstanceOfMetaConcept(var, type);
        } else {
            throw CommonUtil.unreachableStatement("Can't recognize type " + type);
        }
    }

    /**
     * @param label the label of the concept
     * @param var the {@link VarPatternAdmin} representing the concept in the insert query
     * @param sub the supertype property of the var
     * @return a concept with the given ID and the specified type
     */
    private OntologyConcept putOntologyConcept(Label label, VarPatternAdmin var, SubProperty sub) {
        OntologyConcept superConcept = getConcept(sub.getSuperType()).asOntologyConcept();

        if (superConcept.isEntityType()) {
            return graph.putEntityType(label).sup(superConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            return graph.putRelationType(label).sup(superConcept.asRelationType());
        } else if (superConcept.isRole()) {
            return graph.putRole(label).sup(superConcept.asRole());
        } else if (superConcept.isResourceType()) {
            return graph.putResourceType(label, getDataType(var)).sup(superConcept.asResourceType());
        } else if (superConcept.isRuleType()) {
            return graph.putRuleType(label).sup(superConcept.asRuleType());
        } else {
            throw GraqlQueryException.insertMetaType(label, superConcept);
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
    private <T extends Type, S extends Thing> S addOrGetInstance(Optional<ConceptId> id, Supplier<S> addInstance) {
        return id.map(graph::<S>getConcept).orElseGet(addInstance);
    }

    /**
     * Get a label from an optional for a type, throwing an exception if it is not present.
     * This is because types must have specified labels.
     * @param label an optional label to get
     * @return the label, if present
     * @throws GraqlQueryException if the label was not present
     */
    private Label getTypeLabelOrThrow(Optional<Label> label) throws GraqlQueryException {
        return label.orElseThrow(GraqlQueryException::insertTypeWithoutLabel);
    }

    private Object getValue(VarPatternAdmin var) {
        Iterator<ValueProperty> properties = var.getProperties(ValueProperty.class).iterator();

        if (properties.hasNext()) {
            // Value properties are confirmed to be "equals" only in the ValueProperty class
            //noinspection OptionalGetWithoutIsPresent
            Object value = properties.next().getPredicate().equalsValue().get();

            if (properties.hasNext()) {
                throw GraqlQueryException.insertMultipleValues(properties.next().getPredicate(), value);
            }

            return value;
        } else {
            throw GraqlQueryException.insertResourceWithoutValue();
        }
    }

    /**
     * Get the datatype of a {@link VarPatternAdmin} if specified
     * @return the datatype of the given var
     *
     * @throws GraqlQueryException if there is no data type specified
     */
    private ResourceType.DataType<?> getDataType(VarPatternAdmin var) {
        Optional<ResourceType.DataType<?>> directDataType =
                var.getProperty(DataTypeProperty.class).map(DataTypeProperty::getDataType);

        Optional<ResourceType.DataType<?>> indirectDataType =
                getSub(var).map(sub -> getConcept(sub).asResourceType().getDataType());

        Optional<ResourceType.DataType<?>> dataType = optionalOr(directDataType, indirectDataType);

        return dataType.orElseThrow(() -> GraqlQueryException.insertResourceTypeWithoutDataType(var));
    }

    private Optional<VarPatternAdmin> getSub(VarPatternAdmin var) {
        return var.getProperty(SubProperty.class).map(SubProperty::getSuperType);
    }
}
