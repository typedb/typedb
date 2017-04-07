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

import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
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

public abstract class AbstractTypeGenerator<T extends Type> extends FromGraphGenerator<T> {

    private Optional<Boolean> meta = Optional.empty();

    AbstractTypeGenerator(Class<T> type) {
        super(type);
    }

    @Override
    protected final T generateFromGraph() {
        Collection<T> types;

        if (!includeNonMeta()) {
            types = Sets.newHashSet(otherMetaTypes());
            types.add(metaType());
        } else {
            types = (Collection<T>) metaType().subTypes();
        }

        types = types.stream().filter(this::filter).collect(toSet());

        if (!includeMeta()) {
            types.remove(metaType());
            types.removeAll(otherMetaTypes());
        }
        
        if (types.isEmpty() && includeNonMeta()) {
            TypeLabel label = genFromGraph(TypeLabels.class).mustBeUnused().generate(random, status);
            assert graph().getType(label) == null;
            return newType(label);
        } else {
            return random.choose(types);
        }
    }

    protected abstract T newType(TypeLabel label);

    protected abstract T metaType();

    protected Collection<T> otherMetaTypes() {
        return ImmutableSet.of();
    }

    protected boolean filter(T type) {
        return true;
    }

    private final boolean includeMeta() {
        return meta.orElse(true);
    }

    private final boolean includeNonMeta() {
        return !meta.orElse(false);
    }

    final AbstractTypeGenerator<T> excludeMeta() {
        meta = Optional.of(false);
        return this;
    }

    public final void configure(Meta meta) {
        this.meta = Optional.of(meta.value());
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Meta {
        boolean value() default true;
    }
}
