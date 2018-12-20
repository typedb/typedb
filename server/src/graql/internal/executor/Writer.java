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
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.property.PropertyExecutor.WriteExecutor;
import grakn.core.graql.internal.util.Partition;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.StatementImpl;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.Transaction;
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

import static java.util.stream.Collectors.toList;

/**
 * A class for executing PropertyExecutors on VarPropertys within Graql queries.
 * Multiple query types share this class, such as InsertQuery and DefineQuery.
 */
public class Writer {

    protected final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final Transaction transaction;

    // A mutable map associating each `Var` to the `Concept` in the graph it refers to.
    private final Map<Variable, Concept> concepts = new HashMap<>();

    // A mutable map of concepts "under construction" that require more information before they can be built
    private final Map<Variable, ConceptBuilder> conceptBuilders = new HashMap<>();

    // An immutable set of all properties
    private final ImmutableSet<WriteExecutor> properties;

    // A partition (disjoint set) indicating which `Var`s should refer to the same concept
    private final Partition<Variable> equivalentVars;

    // A map, where `dependencies.containsEntry(x, y)` implies that `y` must be inserted before `x` is inserted.
    private final ImmutableMultimap<WriteExecutor, WriteExecutor> dependencies;

    private Writer(Transaction transaction, 
                   Set<WriteExecutor> properties,
                   Partition<Variable> equivalentVars,
                   Multimap<WriteExecutor, WriteExecutor> executorDependency
    ) {
        this.transaction = transaction;
        this.properties = ImmutableSet.copyOf(properties);
        this.equivalentVars = equivalentVars;
        this.dependencies = ImmutableMultimap.copyOf(executorDependency);
    }

    static Writer create(ImmutableSet<WriteExecutor> executors, Transaction transaction) {
        /*
            We build several many-to-many relations, indicated by a `Multimap<X, Y>`. These are used to represent
            the dependencies between properties and variables.

            `propertyToItsRequiredVars.containsEntry(prop, var)` indicates that the property `prop` cannot be inserted until
            the concept represented by the variable `var` is created.

            For example, the property `$x isa $y` depends on the existence of the concept represented by `$y`.
         */
        Multimap<WriteExecutor, Variable> executorToRequiredVars = HashMultimap.create();

        for (WriteExecutor executor : executors) {
            for (Variable requiredVar : executor.requiredVars()) {
                executorToRequiredVars.put(executor, requiredVar);
            }
        }

        /*
            `varToItsProducerProperties.containsEntry(var, prop)` indicates that the concept represented by the variable `var`
            cannot be created until the property `prop` is inserted.

            For example, the concept represented by `$x` will not exist before the property `$x isa $y` is inserted.
         */
        Multimap<Variable, WriteExecutor> varToProducerExecutor = HashMultimap.create();

        for (WriteExecutor executor : executors) {
            for (Variable producedVar : executor.producedVars()) {
                varToProducerExecutor.put(producedVar, executor);
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

                varToItsProducerProperties.containsEntry($X, prop) <=> varToItsProducerProperties.containsEntry($Y, prop)

            Therefore:

                varToItsProducerProperties.containsEntry($X, `$X sub entity`) => varToItsProducerProperties.containsEntry($Y, `$X sub entity`)

            Now we know that `$Y` depends on `$X sub entity` as well as `$X label movie`, which is enough information to
            insert the type!
         */

        Partition<Variable> equivalentVars = Partition.singletons(Collections.emptyList());

        propertyToEquivalentVars(executors).asMap().values().forEach(vars -> {
            // These vars must refer to the same concept, so share their dependencies
            Collection<WriteExecutor> producerExecutors =
                    vars.stream().flatMap(var -> varToProducerExecutor.get(var).stream()).collect(toList());

            Variable first = vars.iterator().next();

            vars.forEach(var -> {
                varToProducerExecutor.replaceValues(var, producerExecutors);
                equivalentVars.merge(first, var);
            });
        });

        /*
            Together, `propertyToItsRequiredVars` and `varToItsProducerProperties` can be composed into a single many-to-many relation:

                propertyDependencies = propertyToItsRequiredVars ∘ varToItsProducerProperties

            By doing so, we map directly between properties, skipping the vars. For example, if we previously had:

                propertyToItsRequiredVars.containsEntry(`$x isa $y`, `$y`);             // `$x isa $y` depends on `$y`
                varToItsProducerProperties.containsEntry(`$y`, `$y label movie`);       // `$y` depends on `$y label movie`

            Then it follows that:

                propertyDependencies.containsEntry(`$x isa $y`, `$y label movie`);      // `$x isa $y` depends on `$y label movie`

            The `propertyDependencies` relation contains all the information to decide what order to execute the properties.
         */
        Multimap<WriteExecutor, WriteExecutor> executorDependency = 
                executorDependency(executorToRequiredVars, varToProducerExecutor);

        return new Writer(transaction, executors, equivalentVars, executorDependency);
    }

    private static Multimap<VarProperty, Variable> propertyToEquivalentVars(Set<WriteExecutor> executors) {
        Multimap<VarProperty, Variable> equivalentProperties = HashMultimap.create();

        for (WriteExecutor executor : executors) {
            if (executor.property().uniquelyIdentifiesConcept()) {
                equivalentProperties.put(executor.property(), executor.var());
            }
        }

        return equivalentProperties;
    }

    /**
     * <a href=https://en.wikipedia.org/wiki/Composition_of_relations>Compose</a> two Multimaps together,
     * treating them like many-to-many relations.
     */
    private static Multimap<WriteExecutor, WriteExecutor> executorDependency(
            Multimap<WriteExecutor, Variable> executorToVar,
            Multimap<Variable, WriteExecutor> varToExecutor
    ) {
        Multimap<WriteExecutor, WriteExecutor> dependency = HashMultimap.create();

        for (Map.Entry<WriteExecutor, Variable> entry1 : executorToVar.entries()) {
            WriteExecutor dependant = entry1.getKey();
            Variable intermediateVar = entry1.getValue();

            for (WriteExecutor depended : varToExecutor.get(intermediateVar)) {
                dependency.put(dependant, depended);
            }
        }

        return dependency;
    }

    ConceptMap write(ConceptMap result) {
        concepts.putAll(result.map());

        for (WriteExecutor executor : sortProperties()) {
            executor.execute(this);
        }

        conceptBuilders.forEach((var, builder) -> buildConcept(var, builder));

        ImmutableMap.Builder<Variable, Concept> allConcepts = ImmutableMap.<Variable, Concept>builder().putAll(concepts);

        // Make sure to include all equivalent vars in the result
        for (Variable var : equivalentVars.getNodes()) {
            allConcepts.put(var, concepts.get(equivalentVars.componentOf(var)));
        }

        Map<Variable, Concept> namedConcepts = Maps.filterKeys(allConcepts.build(), Variable::isUserDefinedName);
        return new ConceptMap(namedConcepts);
    }

    private Concept buildConcept(Variable var, ConceptBuilder builder) {
        Concept concept = builder.build();
        assert concept != null : String.format("build() should never return null. var: %s", var);
        concepts.put(var, concept);
        return concept;
    }

    /**
     * Produce a valid ordering of the properties by using the given dependency information.
     * This method uses a topological sort (Kahn's algorithm) in order to find a valid ordering.
     */
    private ImmutableList<WriteExecutor> sortProperties() {
        ImmutableList.Builder<WriteExecutor> sorted = ImmutableList.builder();

        // invertedDependencies is intended to just be a 'view' on dependencies, so when dependencies is modified
        // we should always also modify invertedDependencies (and vice-versa).
        Multimap<WriteExecutor, WriteExecutor> dependencies = HashMultimap.create(this.dependencies);
        Multimap<WriteExecutor, WriteExecutor> invertedDependencies = HashMultimap.create();
        Multimaps.invertFrom(dependencies, invertedDependencies);

        Queue<WriteExecutor> propertiesWithoutDependencies =
                new ArrayDeque<>(Sets.filter(properties, property -> dependencies.get(property).isEmpty()));

        WriteExecutor property;

        // Retrieve the next property without any dependencies
        while ((property = propertiesWithoutDependencies.poll()) != null) {
            sorted.add(property);

            // We copy this into a new list because the underlying collection gets modified during iteration
            Collection<WriteExecutor> dependents = Lists.newArrayList(invertedDependencies.get(property));

            for (WriteExecutor dependent : dependents) {
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
            Variable var = dependencies.keys().iterator().next().var();
            throw GraqlQueryException.insertRecursive(printableRepresentation(var));
        }

        return sorted.build();
    }

    /**
     * Return a ConceptBuilder for given Variable. This can be used to provide information for how to create
     * the concept that the variable represents.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they return the given Variable in the
     * response to PropertyExecutor#producedVars().
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     *
     * @throws GraqlQueryException if the concept in question has already been created
     */
    public ConceptBuilder getBuilder(Variable var) {
        return tryBuilder(var).orElseThrow(() -> {
            Concept concept = concepts.get(equivalentVars.componentOf(var));
            return GraqlQueryException.insertExistingConcept(printableRepresentation(var), concept);
        });
    }

    /**
     * Return a ConceptBuilder for given Variable. This can be used to provide information for how to create
     * the concept that the variable represents.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they include the given Variable in
     * their PropertyExecutor#producedVars().
     * For example, a property may call {@code executor.builder(var).isa(type);} in order to provide a type for a var.
     * If the concept has already been created, this will return empty.
     */
    public Optional<ConceptBuilder> tryBuilder(Variable var) {
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
     * Return a Concept for a given Variable.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they include the given Variable in
     * their PropertyExecutor#requiredVars().
     */
    public Concept getConcept(Variable var) {
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

    Statement printableRepresentation(Variable var) {
        ImmutableSet.Builder<VarProperty> propertiesOfVar = ImmutableSet.builder();

        // This could be faster if we built a dedicated map Var -> VarPattern
        // However, this method is only used for displaying errors, so it's not worth the cost
        for (WriteExecutor executor : properties) {
            if (executor.var().equals(var)) {
                propertiesOfVar.add(executor.property());
            }
        }

        return new StatementImpl(var, propertiesOfVar.build());
    }

    Transaction tx() {
        return transaction;
    }
}
