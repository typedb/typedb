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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.spanningtree.datastructure.Partition;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import ai.grakn.util.StringUtil;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.flatteningToMultimap;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

/**
 * A class for executing insert queries.
 *
 * This behaviour is moved to its own class to allow InsertQueryImpl to have fewer mutable fields.
 *
 * @author Felix Chapman
 */
public class InsertQueryExecutor {

    private final GraknGraph graph;

    // A map associating each `Var` to the `Concept` in the graph it refers to.
    private final Map<Var, Concept> concepts = new HashMap<>();

    // A map of concepts "under construction" that require more information before they can be built
    private final Map<Var, ConceptBuilder> conceptBuilders = new HashMap<>();

    // An immutable set of all properties
    private final ImmutableSet<VarAndProperty> properties;

    // A mutable set of all properties that have not yet been executed
    private final Set<VarAndProperty> unexecutedProperties;

    // A partition (disjoint set) indicating which `Var`s should refer to the same concept
    private final Partition<Var> equivalentVars;

    // A map, where `dependencies.containsEntry(x, y)` implies that `y` must be inserted before `x` is inserted.
    private final Multimap<VarAndProperty, VarAndProperty> dependencies;

    private InsertQueryExecutor(GraknGraph graph, ImmutableSet<VarAndProperty> properties,
                                Partition<Var> equivalentVars, Multimap<VarAndProperty, VarAndProperty> dependencies) {
        this.graph = graph;
        this.properties = properties;
        this.unexecutedProperties = Sets.newHashSet(properties);
        this.equivalentVars = equivalentVars;
        this.dependencies = dependencies;
    }

    /**
     * Insert all the Vars
     */
    static Answer insertAll(Collection<VarPatternAdmin> patterns, GraknGraph graph) {
        return create(patterns, graph).insertAll(new QueryAnswer());
    }

    /**
     * Insert all the Vars
     * @param results the result of a match query
     */
    static Answer insertAll(Collection<VarPatternAdmin> patterns, GraknGraph graph, Answer results) {
        return create(patterns, graph).insertAll(results);
    }

    private static InsertQueryExecutor create(Collection<VarPatternAdmin> patterns, GraknGraph graph) {
        ImmutableSet<VarAndProperty> properties =
                patterns.stream().flatMap(VarAndProperty::fromPattern).collect(toImmutableSet());

        Multimap<VarAndProperty, Var> propDependencies =
                properties.stream().collect(flatteningToMultimap(identity(), VarAndProperty::requiredVars));

        Multimap<VarAndProperty, Var> producedVars =
                properties.stream().collect(flatteningToMultimap(identity(), VarAndProperty::producedVars));

        Multimap<Var, VarAndProperty> varDependencies = HashMultimap.create();
        Multimaps.invertFrom(producedVars, varDependencies);

        Partition<Var> equivalentVars = Partition.singletons(Collections.emptyList());

        Set<VarAndProperty> identifyingProperties =
                properties.stream().filter(VarAndProperty::uniquelyIdentifiesConcept).collect(toSet());

        identifyingProperties.forEach(vp1 ->
                identifyingProperties.stream().filter(vp2 -> vp1.property().equals(vp2.property())).forEach(vp2 -> {
                    // These properties must refer to the same concept, so share their dependencies
                    Collection<VarAndProperty> producers = varDependencies.get(vp1.var());
                    producers.addAll(varDependencies.get(vp2.var()));
                    varDependencies.replaceValues(vp2.var(), Sets.newHashSet(producers));

                    equivalentVars.merge(vp1.var(), vp2.var());
                }));

//        graphviz(Multimaps.forMap(equivalentVars));

        Multimap<VarAndProperty, VarAndProperty> dependencies = composeMultimaps(propDependencies, varDependencies);
        graphviz(dependencies);

        return new InsertQueryExecutor(graph, properties, equivalentVars, dependencies);
    }

    private static <K, T, V> Multimap<K, V> composeMultimaps(Multimap<K, T> map1, Multimap<T, V> map2) {
        return map1.entries().stream().collect(flatteningToMultimap(Map.Entry::getKey, e -> map2.get(e.getValue()).stream()));
    }

    static <K, V> void graphviz(Multimap<K, V> map) {
        System.out.println("digraph G {");
        map.entries().forEach(entry -> {
            System.out.println("    \"" + StringUtil.escapeString(entry.getKey().toString()) + "\" -> \"" + StringUtil.escapeString(entry.getValue().toString()) + "\"");
        });
        System.out.println("}");
    }

    private Answer insertAll(Answer results) {
        concepts.putAll(results.map());

        // TODO: Make fast (topsort)

        while (!unexecutedProperties.isEmpty()) {
            VarAndProperty executedProperty = null;

            for (VarAndProperty property : unexecutedProperties) {
                Collection<VarAndProperty> dependsOn = dependencies.get(property);

                if (dependsOn.isEmpty()) {

                    executedProperty = property;

                    property.insert(this);

                    break;
                }
            }

            if (executedProperty == null) {
                // Any variables remaining are in a dependency loop, so just display one
                Var var = unexecutedProperties.iterator().next().var();
                throw GraqlQueryException.insertRecursive(printableRepresentation(var));
            }

            unexecutedProperties.remove(executedProperty);

            for (VarAndProperty vp : unexecutedProperties) {
                Collection<VarAndProperty> dependsOn = dependencies.get(vp);
                dependsOn.remove(executedProperty);
            }
        }

        conceptBuilders.forEach((var, builder) -> concepts.put(var, builder.build(this)));

        Map<Var, Concept> namedConcepts = Maps.filterKeys(concepts, Var::isUserDefinedName);
        return new QueryAnswer(namedConcepts);
    }

    public ConceptBuilder builder(Var var) {
        var = equivalentVars.componentOf(var);

        Preconditions.checkArgument(!concepts.containsKey(var));

        ConceptBuilder builder;

        builder = conceptBuilders.get(var);

        if (builder != null) {
            return builder;
        }

        builder = new ConceptBuilder(var);
        conceptBuilders.put(var, builder);
        return builder;
    }

    public Concept get(Var var) {
        var = equivalentVars.componentOf(var);

        Concept concept = concepts.get(var);

        if (concept == null) {
            ConceptBuilder builder = conceptBuilders.remove(var);

            if (builder != null) {
                concept = builder.build(this);
                concepts.put(var, concept);
            }
        }

        if (concept != null) {
            return concept;
        }

        throw GraqlQueryException.insertUndefinedVariable(printableRepresentation(var));
    }

    VarPatternAdmin printableRepresentation(Var var) {
        ImmutableSet.Builder<VarProperty> propertiesOfVar = ImmutableSet.builder();

        for (VarAndProperty vp : properties) {
            if (vp.var().equals(var)) {
                propertiesOfVar.add(vp.property());
            }
        }

        return Patterns.varPattern(var, propertiesOfVar.build());
    }

    GraknGraph graph() {
        return graph;
    }

    @AutoValue
    static abstract class VarAndProperty {
        abstract Var var();
        abstract VarPropertyInternal property();

        static VarAndProperty of(Var var, VarProperty property) {
            return new AutoValue_InsertQueryExecutor_VarAndProperty(var, VarPropertyInternal.from(property));
        }

        static Stream<VarAndProperty> fromPattern(VarPatternAdmin pattern) {
            return pattern.getProperties().map(prop -> VarAndProperty.of(pattern.getVarName(), prop));
        }

        void insert(InsertQueryExecutor executor) {
            property().insert(var(), executor);
        }

        Stream<Var> requiredVars() {
            return property().requiredVars(var()).stream();
        }

        Stream<Var> producedVars() {
            return property().producedVars(var()).stream();
        }

        boolean uniquelyIdentifiesConcept() {
            return property().uniquelyIdentifiesConcept();
        }
    }
}
