/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.executor;

import com.google.common.base.Preconditions;
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
import grakn.benchmark.lib.instrumentation.ServerTracing;
import grakn.core.common.util.Partition;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.ConceptBuilder;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor.Writer;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A class for executing PropertyExecutors on VarProperties within Graql queries.
 * Multiple query types share this class, such as InsertQuery and DefineQuery.
 */
public class WriteExecutorImpl implements WriteExecutor {

    protected final Logger LOG = LoggerFactory.getLogger(WriteExecutor.class);

    private ConceptManager conceptManager;

    // A mutable map associating each `Var` to the `Concept` in the graph it refers to.
    private final Map<Variable, Concept> concepts = new HashMap<>();

    // A set of concepts to be deleted at the end of a write execution
    private final Set<Concept> conceptsToDelete = new HashSet<>();

    // A mutable map of concepts "under construction" that require more information before they can be built
    private final Map<Variable, ConceptBuilder> conceptBuilders = new HashMap<>();

    // An immutable set of all properties
    private final ImmutableSet<Writer> writers;

    // A partition (disjoint set) indicating which `Var`s should refer to the same concept
    private final Partition<Variable> equivalentVars;

    // A map, where `dependencies.containsEntry(x, y)` implies that `y` must be inserted before `x` is inserted.
    private final ImmutableMultimap<Writer, Writer> dependencies;

    private WriteExecutorImpl(ConceptManager conceptManager,
                              Set<Writer> writers,
                              Partition<Variable> equivalentVars,
                              Multimap<Writer, Writer> executorDependency
    ) {
        this.conceptManager = conceptManager;
        this.writers = ImmutableSet.copyOf(writers);
        this.equivalentVars = equivalentVars;
        this.dependencies = ImmutableMultimap.copyOf(executorDependency);
    }

    static WriteExecutor create(ConceptManager conceptManager, ImmutableSet<Writer> writers) {
        /*
            We build several many-to-many relations, indicated by a `Multimap<X, Y>`. These are used to represent
            the dependencies between properties and variables.

            `propertyToItsRequiredVars.containsEntry(prop, var)` indicates that the property `prop` cannot be inserted until
            the concept represented by the variable `var` is created.

            For example, the property `$x isa $y` depends on the existence of the concept represented by `$y`.
         */
        Multimap<Writer, Variable> executorToRequiredVars = HashMultimap.create();

        for (Writer writer : writers) {
            for (Variable requiredVar : writer.requiredVars()) {
                executorToRequiredVars.put(writer, requiredVar);
            }
        }

        /*
            `varToItsProducerProperties.containsEntry(var, prop)` indicates that the concept represented by the variable `var`
            cannot be created until the property `prop` is inserted.

            For example, the concept represented by `$x` will not exist before the property `$x isa $y` is inserted.
         */
        Multimap<Variable, Writer> varToProducingWriter = HashMultimap.create();

        for (Writer executor : writers) {
            for (Variable producedVar : executor.producedVars()) {
                varToProducingWriter.put(producedVar, executor);
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

        propertyToEquivalentVars(writers).asMap().values().forEach(vars -> {
            // These vars must refer to the same concept, so share their dependencies
            Collection<Writer> producingWriters =
                    vars.stream().flatMap(var -> varToProducingWriter.get(var).stream()).collect(toList());

            Variable first = vars.iterator().next();

            vars.forEach(var -> {
                varToProducingWriter.replaceValues(var, producingWriters);
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
        Multimap<Writer, Writer> writerDependencies =
                writerDependencies(executorToRequiredVars, varToProducingWriter);

        return new WriteExecutorImpl(conceptManager, writers, equivalentVars, writerDependencies);
    }

    private static Multimap<VarProperty, Variable> propertyToEquivalentVars(Set<Writer> executors) {
        Multimap<VarProperty, Variable> equivalentProperties = HashMultimap.create();

        for (Writer executor : executors) {
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
    private static Multimap<Writer, Writer> writerDependencies(
            Multimap<Writer, Variable> writerToVar,
            Multimap<Variable, Writer> varToWriter
    ) {
        Multimap<Writer, Writer> dependency = HashMultimap.create();

        for (Map.Entry<Writer, Variable> entry : writerToVar.entries()) {
            Writer dependant = entry.getKey();
            Variable intermediateVar = entry.getValue();

            for (Writer depended : varToWriter.get(intermediateVar)) {
                dependency.put(dependant, depended);
            }
        }

        return dependency;
    }

    public Stream<ConceptMap> write() {
        return write(new ConceptMap());
    }

    public Stream<ConceptMap> write(ConceptMap preExisting) {
        concepts.putAll(preExisting.map());

        // time to execute writers for properties
        int executeWritersSpanId = ServerTracing.startScopedChildSpan("WriteExecutor.write execute writers");


        for (Writer writer : sortedWriters()) {
            writer.execute(this);
        }

        ServerTracing.closeScopedChildSpan(executeWritersSpanId);
        // time to delete concepts marked for deletion

        int deleteConceptsSpanId = ServerTracing.startScopedChildSpan("WriteExecutor.write delete concepts");


        for (Concept concept : conceptsToDelete) {
            concept.delete();
        }

        ServerTracing.closeScopedChildSpan(deleteConceptsSpanId);

        // time to build concepts
        int buildConceptsSpanId = ServerTracing.startScopedChildSpan("WriteExecutor.write build concepts for answer");

        conceptBuilders.forEach((var, builder) -> buildConcept(var, builder));

        ServerTracing.closeScopedChildSpan(buildConceptsSpanId);

        int answerAndPersistId = ServerTracing.startScopedChildSpan("WriteExecutor.write create answers and persist dependent inferred concepts");

        ImmutableMap.Builder<Variable, Concept> allConcepts = ImmutableMap.<Variable, Concept>builder().putAll(concepts);

        // Make sure to include all equivalent vars in the result
        for (Variable var : equivalentVars.getNodes()) {
            allConcepts.put(var, concepts.get(equivalentVars.componentOf(var)));
        }

        Map<Variable, Concept> namedConcepts = Maps.filterKeys(allConcepts.build(), Variable::isReturned);


        ServerTracing.closeScopedChildSpan(answerAndPersistId);

        return Stream.of(new ConceptMap(namedConcepts));
    }


    @Override
    public void toDelete(Concept concept) {
        conceptsToDelete.add(concept);
    }

    private Concept buildConcept(Variable var, ConceptBuilder builder) {
        Concept concept = builder.build();
        Preconditions.checkNotNull(concept, "build() should never return null. var: %s", var);
        concepts.put(var, concept);
        return concept;
    }

    /**
     * Produce a valid ordering of the properties by using the given dependency information.
     * This method uses a topological sort (Kahn's algorithm) in order to find a valid ordering.
     */
    private ImmutableList<Writer> sortedWriters() {
        ImmutableList.Builder<Writer> sorted = ImmutableList.builder();

        // invertedDependencies is intended to just be a 'view' on dependencies, so when dependencies is modified
        // we should always also modify invertedDependencies (and vice-versa).
        Multimap<Writer, Writer> dependencies = HashMultimap.create(this.dependencies);
        Multimap<Writer, Writer> invertedDependencies = HashMultimap.create();
        Multimaps.invertFrom(dependencies, invertedDependencies);

        Queue<Writer> writerWithoutDependencies =
                new ArrayDeque<>(Sets.filter(writers, property -> dependencies.get(property).isEmpty()));

        Writer property;

        // Retrieve the next property without any dependencies
        while ((property = writerWithoutDependencies.poll()) != null) {
            sorted.add(property);

            // We copy this into a new list because the underlying collection gets modified during iteration
            Collection<Writer> dependents = Lists.newArrayList(invertedDependencies.get(property));

            for (Writer dependent : dependents) {
                // Because the property has been removed, the dependent no longer needs to depend on it
                dependencies.remove(dependent, property);
                invertedDependencies.remove(property, dependent);

                boolean hasNoDependencies = dependencies.get(dependent).isEmpty();

                if (hasNoDependencies) {
                    writerWithoutDependencies.add(dependent);
                }
            }
        }

        if (!dependencies.isEmpty()) {
            // This means there must have been a loop. Pick an arbitrary remaining var to display
            Variable var = dependencies.keys().iterator().next().var();
            throw GraqlSemanticException.insertRecursive(printableRepresentation(var));
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
     * @throws GraqlSemanticException if the concept in question has already been created
     */
    @Override
    public ConceptBuilder getBuilder(Variable var) {
        return tryBuilder(var).orElseThrow(() -> {
            Concept concept = concepts.get(equivalentVars.componentOf(var));
            return GraqlSemanticException.insertExistingConcept(printableRepresentation(var), concept);
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
    @Override
    public Optional<ConceptBuilder> tryBuilder(Variable var) {
        var = equivalentVars.componentOf(var);

        if (concepts.containsKey(var)) {
            return Optional.empty();
        }

        ConceptBuilder builder = conceptBuilders.get(var);

        if (builder != null) {
            return Optional.of(builder);
        }

        builder = ConceptBuilderImpl.of(conceptManager, this, var);
        conceptBuilders.put(var, builder);
        return Optional.of(builder);
    }

    /**
     * Return a Concept for a given Variable.
     * This method is expected to be called from implementations of
     * VarProperty#insert(Variable), provided they include the given Variable in
     * their PropertyExecutor#requiredVars().
     */
    @Override
    public Concept getConcept(Variable var) {
        var = equivalentVars.componentOf(var);
        Preconditions.checkNotNull(var);

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

        throw GraqlSemanticException.insertUndefinedVariable(printableRepresentation(var));
    }

    @Override
    public boolean isConceptDefined(Variable var) {
        var = equivalentVars.componentOf(var);
        return concepts.containsKey(var);
    }

    @Override
    public Statement printableRepresentation(Variable var) {
        LinkedHashSet<VarProperty> propertiesOfVar = new LinkedHashSet<>();

        // This could be faster if we built a dedicated map Var -> VarPattern
        // However, this method is only used for displaying errors, so it's not worth the cost
        for (Writer executor : writers) {
            if (executor.var().equals(var)) {
                propertiesOfVar.add(executor.property());
            }
        }

        return Statement.create(var, propertiesOfVar);
    }
}
