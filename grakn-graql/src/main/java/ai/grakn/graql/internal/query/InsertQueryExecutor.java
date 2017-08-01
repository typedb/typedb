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
import ai.grakn.graql.internal.gremlin.spanningtree.util.Pair;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.flatteningToMultimap;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static java.util.function.Function.identity;

/**
 * A class for executing insert queries.
 *
 * This behaviour is moved to its own class to allow InsertQueryImpl to have fewer mutable fields.
 *
 * @author Felix Chapman
 */
public class InsertQueryExecutor {

    private final GraknGraph graph;

    // A mutable map associating each `Var` to the `Concept` in the graph it refers to.
    private final Map<Var, Concept> concepts = new HashMap<>();

    // A mutable map of concepts "under construction" that require more information before they can be built
    private final Map<Var, ConceptBuilder> conceptBuilders = new HashMap<>();

    // An immutable set of all properties
    private final ImmutableSet<VarAndProperty> properties;

    // A partition (disjoint set) indicating which `Var`s should refer to the same concept
    private final Partition<Var> equivalentVars;

    // A map, where `dependencies.containsEntry(x, y)` implies that `y` must be inserted before `x` is inserted.
    private final ImmutableMultimap<VarAndProperty, VarAndProperty> dependencies;

    private InsertQueryExecutor(GraknGraph graph, ImmutableSet<VarAndProperty> properties,
                                Partition<Var> equivalentVars,
                                ImmutableMultimap<VarAndProperty, VarAndProperty> dependencies) {
        this.graph = graph;
        this.properties = properties;
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

        /*
            We build several many-to-many relations, indicated by a `Multimap<X, Y>`. These are used to represent
            the dependencies between properties and variables.

            `propDependencies.containsEntry(prop, var)` indicates that the property `prop` cannot be inserted until
            the concept represented by the variable `var` is created.

            For example, the property `$x isa $y` depends on the existence of the concept represented by `$y`.
         */

        Multimap<VarAndProperty, Var> propDependencies =
                properties.stream().collect(flatteningToMultimap(identity(), VarAndProperty::requiredVars));

        /*
            `varDependencies.containsEntry(var, prop)` indicates that the concept represented by the variable `var`
            cannot be created until the property `prop` is inserted.

            For example, the concept represented by `$x` will not exist before the property `$x isa $y` is inserted.
         */
        Multimap<Var, VarAndProperty> varDependencies = HashMultimap.create();

        Multimap<VarAndProperty, Var> producedVars =
                properties.stream().collect(flatteningToMultimap(identity(), VarAndProperty::producedVars));

        Multimaps.invertFrom(producedVars, varDependencies);

        /*
            Equivalent vars are variables that must represent the same concept as another var.

                 $x label movie, sub entity;
                 $y label movie;
                 $z isa $y;

            In this example, `$z isa $y` must not be inserted before `$y` is. However, `$y` does not have enough
            information to insert on its own! We know `$y` must represent the same concept as `$x`, because they
            both share the same label property. Therefore, we can share their dependencies, such that:

                varDependencies.containsEntry($x, prop) <=> varDependencies.containsEntry($y, prop)

            Therefore:

                varDependencies.containsEntry($x, `$x sub entity`) => varDependencies.containsEntry($y, `$x sub entity`)

            Now we know that `$y` depends on `$x sub entity` as well as `$x label movie`, which is enough information to
            insert the type!
         */

        Partition<Var> equivalentVars = Partition.singletons(Collections.emptyList());

        equivalentProperties(properties).forEach(props -> {
            // These properties must refer to the same concept, so share their dependencies
            Collection<VarAndProperty> producers = varDependencies.get(props.first.var());
            producers.addAll(varDependencies.get(props.second.var()));
            varDependencies.replaceValues(props.second.var(), Sets.newHashSet(producers));

            equivalentVars.merge(props.first.var(), props.second.var());
        });

        /*
            Together, `propDependencies` and `varDependencies` can be composed into a single many-to-many relation:

                dependencies = propDependencies ∘ varDependencies

            By doing so, we map _directly_ between properties, skipping the vars. For example, if we previously had:

                propDependencies.containsEntry(`$x isa $y`, `$y`);         // `$x isa $y` depends on `$y`
                varDependencies.containsEntry(`$y`, `$y label movie`);     // `$y` depends on `$y label movie`

            Then it follows that:

                dependencies.containsEntry(`$x isa $y`, `$y label movie`); // `$x isa $y` depends on `$y label movie`

            The `dependencies` relation contains all the information to decide what order to execute the properties.
         */
        Multimap<VarAndProperty, VarAndProperty> dependencies = composeMultimaps(propDependencies, varDependencies);

        return new InsertQueryExecutor(graph, properties, equivalentVars, ImmutableMultimap.copyOf(dependencies));
    }

    private static Stream<Pair<VarAndProperty, VarAndProperty>> equivalentProperties(Set<VarAndProperty> properties) {
        Set<VarAndProperty> identifyingProperties = Sets.filter(properties, VarAndProperty::uniquelyIdentifiesConcept);

        return identifyingProperties.stream()
                .flatMap(vp1 -> identifyingProperties.stream()
                        .filter(vp2 -> vp1.property().equals(vp2.property())).map(vp2 -> Pair.of(vp1, vp2))
                );
    }

    /**
     * <a href=https://en.wikipedia.org/wiki/Composition_of_relations>Compose</a> two {@link Multimap}s together,
     * treating them like many-to-many relations.
     */
    private static <K, T, V> Multimap<K, V> composeMultimaps(Multimap<K, T> map1, Multimap<T, V> map2) {
        return map1.entries()
                .stream()
                .collect(flatteningToMultimap(Map.Entry::getKey, e -> map2.get(e.getValue()).stream()));
    }

    private Answer insertAll(Answer results) {
        concepts.putAll(results.map());

        sortProperties().forEach(property -> property.insert(this));

        conceptBuilders.forEach((var, builder) -> concepts.put(var, builder.build()));

        Map<Var, Concept> namedConcepts = Maps.filterKeys(concepts, Var::isUserDefinedName);
        return new QueryAnswer(namedConcepts);
    }

    /**
     * Produce a valid ordering of the properties by using the given dependency information.
     *
     * <p>
     *     This method uses a topological sort (Kahn's algorithm) in order to find a valid ordering.
     * </p>
     */
    private ImmutableList<VarAndProperty> sortProperties() {
        ImmutableList.Builder<VarAndProperty> sorted = ImmutableList.builder();

        // invertedDependencies is intended to just be a 'view' on dependencies, so when dependencies is modified
        // we should always also modify invertedDependencies (and vice-versa).
        Multimap<VarAndProperty, VarAndProperty> dependencies = HashMultimap.create(this.dependencies);
        Multimap<VarAndProperty, VarAndProperty> invertedDependencies = HashMultimap.create();
        Multimaps.invertFrom(dependencies, invertedDependencies);

        Queue<VarAndProperty> propertiesWithoutDependencies =
                new ArrayDeque<>(Sets.filter(properties, property -> dependencies.get(property).isEmpty()));

        VarAndProperty property;

        while ((property = propertiesWithoutDependencies.poll()) != null) {
            sorted.add(property);

            // We copy this into a new list because the underlying collection gets modified during iteration
            Collection<VarAndProperty> dependents = Lists.newArrayList(invertedDependencies.get(property));

            for (VarAndProperty dependent : dependents) {
                dependencies.remove(dependent, property);
                invertedDependencies.remove(property, dependent);

                boolean hasNoDependencies = dependencies.get(dependent).isEmpty();

                if (hasNoDependencies) {
                    propertiesWithoutDependencies.add(dependent);
                }
            }
        }

        if (!dependencies.isEmpty()) {
            // This means there must have been a loop. Pick an arbitrary remaining var to display
            Var var = dependencies.keys().iterator().next().var();
            throw GraqlQueryException.insertRecursive(printableRepresentation(var));
        }

        return sorted.build();
    }

    /**
     * Return a {@link ConceptBuilder} for given {@link Var}. This can be used to provide information for how to create
     * the concept that the variable represents.
     *
     * <p>
     * This method is expected to be called from implementations of
     * {@link VarPropertyInternal#insert(Var, InsertQueryExecutor)}, provided they return the given {@link Var} in the
     * response to {@link VarPropertyInternal#producedVars(Var)}.
     * </p>
     * <p>
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     * </p>
     */
    public ConceptBuilder builder(Var var) {
        var = equivalentVars.componentOf(var);

        Preconditions.checkArgument(!concepts.containsKey(var));

        ConceptBuilder builder;

        builder = conceptBuilders.get(var);

        if (builder != null) {
            return builder;
        }

        builder = ConceptBuilder.of(this, var);
        conceptBuilders.put(var, builder);
        return builder;
    }

    /**
     * Return a {@link Concept} for a given {@link Var}.
     *
     * <p>
     * This method is expected to be called from implementations of
     * {@link VarPropertyInternal#insert(Var, InsertQueryExecutor)}, provided they return the given {@link Var} in the
     * response to {@link VarPropertyInternal#requiredVars(Var)}.
     * </p>
     */
    public Concept get(Var var) {
        var = equivalentVars.componentOf(var);

        Concept concept = concepts.get(var);

        if (concept == null) {
            ConceptBuilder builder = conceptBuilders.remove(var);

            if (builder != null) {
                concept = builder.build();
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

        // TODO: probs slow
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
