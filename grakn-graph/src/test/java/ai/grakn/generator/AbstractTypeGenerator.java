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
import ai.grakn.concept.TypeName;
import com.google.common.collect.ImmutableSet;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.toSet;

public abstract class AbstractTypeGenerator<T extends Type> extends FromGraphGenerator<T> {

    private boolean excludeMeta = false;

    AbstractTypeGenerator(Class<T> type) {
        super(type);
    }

    @Override
    protected final T generateFromGraph() {
        Collection<T> types = (Collection<T>) metaType().subTypes();

        types = types.stream().filter(this::filter).collect(toSet());

        if (excludeMeta) {
            types.remove(metaType());
            types.removeAll(otherMetaTypes());
        }
        
        if (types.isEmpty()) {
            TypeName name = genFromGraph(TypeNames.class).mustBeUnused().generate(random, status);
            assert graph().getType(name) == null;
            return newType(name);
        } else {
            return random.choose(types);
        }
    }

    protected abstract T newType(TypeName name);

    protected abstract T metaType();

    protected Collection<T> otherMetaTypes() {
        return ImmutableSet.of();
    }

    protected boolean filter(T type) {
        return true;
    }

    public final void configure(NotMeta notMeta) {
        excludeMeta();
    }

    final AbstractTypeGenerator<T> excludeMeta() {
        this.excludeMeta = true;
        return this;
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public static @interface NotMeta {
    }
}
