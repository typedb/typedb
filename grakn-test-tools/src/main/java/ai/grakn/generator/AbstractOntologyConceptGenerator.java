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
 *
 */

package ai.grakn.generator;

import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Optional;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.toSet;

/**
 * Abstract class for generating {@link OntologyConcept}s.
 *
 * @param <T> the kind of {@link OntologyConcept} to generate
 *
 * @author Felix Chapman
 */
public abstract class AbstractOntologyConceptGenerator<T extends OntologyConcept> extends FromGraphGenerator<T> {

    private Optional<Boolean> meta = Optional.empty();

    AbstractOntologyConceptGenerator(Class<T> type) {
        super(type);
    }

    @Override
    protected final T generateFromGraph() {
        Collection<T> ontologyConcepts;

        if (!includeNonMeta()) {
            ontologyConcepts = Sets.newHashSet(otherMetaOntologyConcepts());
            ontologyConcepts.add(metaOntologyConcept());
        } else {
            ontologyConcepts = (Collection<T>) metaOntologyConcept().subs().collect(toSet());
        }

        ontologyConcepts = ontologyConcepts.stream().filter(this::filter).collect(toSet());

        if (!includeMeta()) {
            ontologyConcepts.remove(metaOntologyConcept());
            ontologyConcepts.removeAll(otherMetaOntologyConcepts());
        }

        if (ontologyConcepts.isEmpty() && includeNonMeta()) {
            Label label = genFromGraph(Labels.class).mustBeUnused().generate(random, status);
            assert graph().getOntologyConcept(label) == null;
            return newOntologyConcept(label);
        } else {
            return random.choose(ontologyConcepts);
        }
    }

    protected abstract T newOntologyConcept(Label label);

    protected abstract T metaOntologyConcept();

    protected Collection<T> otherMetaOntologyConcepts() {
        return ImmutableSet.of();
    }

    protected boolean filter(T ontologyConcept) {
        return true;
    }

    private final boolean includeMeta() {
        return meta.orElse(true);
    }

    private final boolean includeNonMeta() {
        return !meta.orElse(false);
    }

    final AbstractOntologyConceptGenerator<T> excludeMeta() {
        meta = Optional.of(false);
        return this;
    }

    public final void configure(Meta meta) {
        Preconditions.checkArgument(
                !this.meta.isPresent() || this.meta.get(), "Cannot specify parameter is both meta and non-meta");
        this.meta = Optional.of(true);
    }

    public final void configure(NonMeta nonMeta) {
        Preconditions.checkArgument(
                !this.meta.isPresent() || !this.meta.get(), "Cannot specify parameter is both meta and non-meta");
        this.meta = Optional.of(false);
    }

    /**
     * Specify whether the generated {@link OntologyConcept} should be a meta concept
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Meta {
    }

    /**
     * Specify whether the generated {@link OntologyConcept} should not be a meta concept
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface NonMeta {
    }
}
