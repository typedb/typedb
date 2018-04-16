/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.generator;

/*-
 * #%L
 * grakn-test-tools
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
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
 * Abstract class for generating {@link SchemaConcept}s.
 *
 * @param <T> the kind of {@link SchemaConcept} to generate
 *
 * @author Felix Chapman
 */
@SuppressWarnings("unused")
public abstract class AbstractSchemaConceptGenerator<T extends SchemaConcept> extends FromTxGenerator<T> {

    private Optional<Boolean> meta = Optional.empty();

    AbstractSchemaConceptGenerator(Class<T> type) {
        super(type);
    }

    @Override
    protected final T generateFromTx() {
        Collection<T> schemaConcepts;

        if (!includeNonMeta()) {
            schemaConcepts = Sets.newHashSet(otherMetaSchemaConcepts());
            schemaConcepts.add(metaSchemaConcept());
        } else {
            schemaConcepts = (Collection<T>) metaSchemaConcept().subs().collect(toSet());
        }

        schemaConcepts = schemaConcepts.stream().filter(this::filter).collect(toSet());

        if (!includeMeta()) {
            schemaConcepts.remove(metaSchemaConcept());
            schemaConcepts.removeAll(otherMetaSchemaConcepts());
        }

        if (schemaConcepts.isEmpty() && includeNonMeta()) {
            Label label = genFromTx(Labels.class).mustBeUnused().generate(random, status);
            assert tx().getSchemaConcept(label) == null;
            return newSchemaConcept(label);
        } else {
            return random.choose(schemaConcepts);
        }
    }

    protected abstract T newSchemaConcept(Label label);

    protected abstract T metaSchemaConcept();

    protected Collection<T> otherMetaSchemaConcepts() {
        return ImmutableSet.of();
    }

    protected boolean filter(T schemaConcept) {
        return true;
    }

    private final boolean includeMeta() {
        return meta.orElse(true);
    }

    private final boolean includeNonMeta() {
        return !meta.orElse(false);
    }

    final AbstractSchemaConceptGenerator<T> excludeMeta() {
        meta = Optional.of(false);
        return this;
    }

    public final void configure(@SuppressWarnings("unused") Meta meta) {
        Preconditions.checkArgument(
                !this.meta.isPresent() || this.meta.get(), "Cannot specify parameter is both meta and non-meta");
        this.meta = Optional.of(true);
    }

    public final void configure(@SuppressWarnings("unused") NonMeta nonMeta) {
        Preconditions.checkArgument(
                !this.meta.isPresent() || !this.meta.get(), "Cannot specify parameter is both meta and non-meta");
        this.meta = Optional.of(false);
    }

    /**
     * Specify whether the generated {@link SchemaConcept} should be a meta concept
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Meta {
    }

    /**
     * Specify whether the generated {@link SchemaConcept} should not be a meta concept
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface NonMeta {
    }
}
