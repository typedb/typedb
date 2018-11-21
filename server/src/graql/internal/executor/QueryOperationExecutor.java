/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.executor;

import grakn.core.server.Transaction;
import grakn.core.graql.concept.Concept;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.DefineQuery;
import grakn.core.graql.InsertQuery;
import grakn.core.graql.Query;
import grakn.core.graql.Var;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.admin.VarProperty;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.internal.pattern.Patterns;
import grakn.core.graql.internal.pattern.property.PropertyExecutor;
import grakn.core.graql.internal.pattern.property.VarPropertyInternal;
import grakn.core.graql.query.answer.ConceptMapImpl;
import grakn.core.graql.internal.util.Partition;
import com.google.auto.value.AutoValue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

/**
 * A class for executing {@link PropertyExecutor}s on {@link VarProperty}s within {@link Query}s.
 * Multiple query types share this class, such as {@link InsertQuery} and {@link DefineQuery}.
 */
public class QueryOperationExecutor {

    protected final Logger LOG = LoggerFactory.getLogger(QueryOperationExecutor.class);

    private final Transaction tx;

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

    private QueryOperationExecutor(Transaction tx, ImmutableSet<VarAndProperty> properties,
                                   Partition<Var> equivalentVars,
                                   ImmutableMultimap<VarAndProperty, VarAndProperty> dependencies) {
        this.tx = tx;
        this.properties = properties;
        this.equivalentVars = equivalentVars;
        this.dependencies = dependencies;
    }

    /**
     * Insert all the Vars
     */
    static ConceptMap insertAll(Collection<VarPatternAdmin> patterns, Transaction graph) {
        return create(patterns, graph, ExecutionType.INSERT).insertAll(new ConceptMapImpl());
    }

    /**
     * Insert all the Vars
     * @param results the result after inserting
     */
    static ConceptMap insertAll(Collection<VarPatternAdmin> patterns, Transaction graph, ConceptMap results) {
        return create(patterns, graph, ExecutionType.INSERT).insertAll(results);
    }

    static ConceptMap defineAll(Collection<VarPatternAdmin> patterns, Transaction graph) {
        return create(patterns, graph, ExecutionType.DEFINE).insertAll(new ConceptMapImpl());
    }

    static ConceptMap undefineAll(ImmutableList<VarPatternAdmin> patterns, Transaction tx) {
        return create(patterns, tx, ExecutionType.UNDEFINE).insertAll(new ConceptMapImpl());
    }

    private static QueryOperationExecutor create(
            Collection<VarPatternAdmin> patterns, Transaction graph, ExecutionType executionType
    ) {
        ImmutableSet<VarAndProperty> properties = patterns.stream()
                .flatMap(pattern -> VarAndProperty.fromPattern(pattern, executionType))
                .collect(toImmutableSet());

        /*
            We build several many-to-many relations, indicated by a `Multimap<X, Y>`. These are used to represent
            the dependencies between properties and variables.

            `propDependencies.containsEntry(prop, var)` indicates that the property `prop` cannot be inserted until
            the concept represented by the variable `var` is created.

            For example, the property `$x isa $y` depends on the existence of the concept represented by `$y`.
         */
        Multimap<VarAndProperty, Var> propDependencies = HashMultimap.create();

        for (VarAndProperty property : properties) {
            for (Var requiredVar : property.executor().requiredVars()) {
                propDependencies.put(property, requiredVar);
            }
        }

        /*
            `varDependencies.containsEntry(var, prop)` indicates that the concept represented by the variable `var`
            cannot be created until the property `prop` is inserted.

            For example, the concept represented by `$x` will not exist before the property `$x isa $y` is inserted.
         */
        Multimap<Var, VarAndProperty> varDependencies = HashMultimap.create();

        for (VarAndProperty property : properties) {
            for (Var producedVar : property.executor().producedVars()) {
                varDependencies.put(producedVar, property);
            }
        }

        /*
            Equivalent vars are variables that must represent the same concept as another var.

                 $X label movie, sub entity;
                 $Y label movie;
                 $z isa $Y;

            In this example, `$z isa $Y` must not be inserted before `$Y` is. However, `$Y` does not have enough
            information to insert on its own. It also needs a super type!

            We know `$Y` must represent the same concept as `$X`, because they both share the same label property.
            Therefore, we can share their dependencies, such that:

                varDependencies.containsEntry($X, prop) <=> varDependencies.containsEntry($Y, prop)

            Therefore:

                varDependencies.containsEntry($X, `$X sub entity`) => varDependencies.containsEntry($Y, `$X sub entity`)

            Now we know that `$Y` depends on `$X sub entity` as well as `$X label movie`, which is enough information to
            insert the type!
         */

        Partition<Var> equivalentVars = Partition.singletons(Collections.emptyList());

        equivalentProperties(properties).asMap().values().forEach(vars -> {
            // These vars must refer to the same concept, so share their dependencies
            Collection<VarAndProperty> producers =
                    vars.stream().flatMap(var -> varDependencies.get(var).stream()).collect(toList());

            Var first = vars.iterator().next();

            vars.forEach(var -> {
                varDependencies.replaceValues(var, producers);
                equivalentVars.merge(first, var);
            });
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

        return new QueryOperationExecutor(graph, properties, equivalentVars, ImmutableMultimap.copyOf(dependencies));
    }

    private static Multimap<VarProperty, Var> equivalentProperties(Set<VarAndProperty> properties) {
        Multimap<VarProperty, Var> equivalentProperties = HashMultimap.create();

        for (VarAndProperty varAndProperty : properties) {
            if (varAndProperty.uniquelyIdentifiesConcept()) {
                equivalentProperties.put(varAndProperty.property(), varAndProperty.var());
            }
        }

        return equivalentProperties;
    }

    /**
     * <a href=https://en.wikipedia.org/wiki/Composition_of_relations>Compose</a> two {@link Multimap}s together,
     * treating them like many-to-many relations.
     */
    private static <K, T, V> Multimap<K, V> composeMultimaps(Multimap<K, T> map1, Multimap<T, V> map2) {
        Multimap<K, V> composed = HashMultimap.create();

        for (Map.Entry<K, T> entry1 : map1.entries()) {
            K key = entry1.getKey();
            T intermediateValue = entry1.getValue();

            for (V value : map2.get(intermediateValue)) {
                composed.put(key, value);
            }
        }

        return composed;
    }

    private ConceptMap insertAll(ConceptMap map) {
        concepts.putAll(map.map());

        for (VarAndProperty property : sortProperties()) {
            property.executor().execute(this);
        }

        conceptBuilders.forEach(this::buildConcept);

        ImmutableMap.Builder<Var, Concept> allConcepts = ImmutableMap.<Var, Concept>builder().putAll(concepts);

        // Make sure to include all equivalent vars in the result
        for (Var var: equivalentVars.getNodes()) {
            allConcepts.put(var, concepts.get(equivalentVars.componentOf(var)));
        }

        Map<Var, Concept> namedConcepts = Maps.filterKeys(allConcepts.build(), Var::isUserDefinedName);
        return new ConceptMapImpl(namedConcepts);
    }

    private Concept buildConcept(Var var, ConceptBuilder builder) {
        Concept concept = builder.build();
        assert concept != null : String.format("build() should never return null. var: %s", var);
        concepts.put(var, concept);
        return concept;
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

        // Retrieve the next property without any dependencies
        while ((property = propertiesWithoutDependencies.poll()) != null) {
            sorted.add(property);

            // We copy this into a new list because the underlying collection gets modified during iteration
            Collection<VarAndProperty> dependents = Lists.newArrayList(invertedDependencies.get(property));

            for (VarAndProperty dependent : dependents) {
                // Because the property has been removed, the dependent no longer needs to depend on it
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
     * {@link VarPropertyInternal#insert(Var)}, provided they return the given {@link Var} in the
     * response to {@link PropertyExecutor#producedVars()}.
     * </p>
     * <p>
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     * </p>
     *
     * @throws GraqlQueryException if the concept in question has already been created
     */
    public ConceptBuilder builder(Var var) {
        return tryBuilder(var).orElseThrow(() -> {
            Concept concept = concepts.get(equivalentVars.componentOf(var));
            return GraqlQueryException.insertExistingConcept(printableRepresentation(var), concept);
        });
    }

    /**
     * Return a {@link ConceptBuilder} for given {@link Var}. This can be used to provide information for how to create
     * the concept that the variable represents.
     *
     * <p>
     * This method is expected to be called from implementations of
     * {@link VarPropertyInternal#insert(Var)}, provided they return the given {@link Var} in the
     * response to {@link PropertyExecutor#producedVars()}.
     * </p>
     * <p>
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     * </p>
     * <p>
     *     If the concept has already been created, this will return empty.
     * </p>
     */
    public Optional<ConceptBuilder> tryBuilder(Var var) {
        var = equivalentVars.componentOf(var);

        if (concepts.containsKey(var)) {
            return Optional.empty();
        }

        ConceptBuilder builder = conceptBuilders.get(var);

        if (builder != null) {
            return Optional.of(builder);
        }

        builder = ConceptBuilder.of(this, var);
        conceptBuilders.put(var, builder);
        return Optional.of(builder);
    }

    /**
     * Return a {@link Concept} for a given {@link Var}.
     *
     * <p>
     * This method is expected to be called from implementations of
     * {@link VarPropertyInternal#insert(Var)}, provided they return the given {@link Var} in the
     * response to {@link PropertyExecutor#requiredVars()}.
     * </p>
     */
    public Concept get(Var var) {
        var = equivalentVars.componentOf(var);
        assert var != null;

        @Nullable Concept concept = concepts.get(var);

        if (concept == null) {
            @Nullable ConceptBuilder builder = conceptBuilders.remove(var);

            if (builder != null) {
                concept = buildConcept(var, builder);
            }
        }

        if (concept != null) {
            return concept;
        }

        LOG.debug("Could not build concept for {}\nconcepts = {}\nconceptBuilders = {}", var, concepts, conceptBuilders);

        throw GraqlQueryException.insertUndefinedVariable(printableRepresentation(var));
    }

    VarPatternAdmin printableRepresentation(Var var) {
        ImmutableSet.Builder<VarProperty> propertiesOfVar = ImmutableSet.builder();

        // This could be faster if we built a dedicated map Var -> VarPattern
        // However, this method is only used for displaying errors, so it's not worth the cost
        for (VarAndProperty vp : properties) {
            if (vp.var().equals(var)) {
                propertiesOfVar.add(vp.property());
            }
        }

        return Patterns.varPattern(var, propertiesOfVar.build());
    }

    Transaction tx() {
        return tx;
    }

    /**
     * Represents a pairing of a {@link VarProperty} and its subject {@link Var}.
     * <p>
     *     e.g. {@code $x} and {@code isa $y}, together are {@code $x isa $y}.
     * </p>
     */
    @AutoValue
    static abstract class VarAndProperty {

        abstract Var var();
        abstract VarPropertyInternal property();
        abstract PropertyExecutor executor();

        private static VarAndProperty of(Var var, VarProperty property, PropertyExecutor executor) {
            VarPropertyInternal propertyInternal = VarPropertyInternal.from(property);
            return new AutoValue_QueryOperationExecutor_VarAndProperty(var, propertyInternal, executor);
        }

        private static Stream<VarAndProperty> all(Var var, VarProperty property, ExecutionType executionType) {
            VarPropertyInternal propertyInternal = VarPropertyInternal.from(property);
            return executionType.executors(propertyInternal, var).stream()
                    .map(executor -> VarAndProperty.of(var, property, executor));
        }

        static Stream<VarAndProperty> fromPattern(VarPatternAdmin pattern, ExecutionType executionType) {
            return pattern.getProperties().flatMap(prop -> VarAndProperty.all(pattern.var(), prop, executionType));
        }

        boolean uniquelyIdentifiesConcept() {
            return property().uniquelyIdentifiesConcept();
        }
    }

    private enum ExecutionType {
        INSERT {
            Collection<PropertyExecutor> executors(VarPropertyInternal property, Var var) {
                return property.insert(var);
            }
        },
        DEFINE {
            Collection<PropertyExecutor> executors(VarPropertyInternal property, Var var) {
                return property.define(var);
            }
        },
        UNDEFINE {
            Collection<PropertyExecutor> executors(VarPropertyInternal property, Var var) {
                return property.undefine(var);
            }
        };

        abstract Collection<PropertyExecutor> executors(VarPropertyInternal property, Var var);
    }
}
