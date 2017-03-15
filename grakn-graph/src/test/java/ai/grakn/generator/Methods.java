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

import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;
import org.mockito.Mockito;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.stream.Stream;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class Methods extends AbstractGenerator<Method> {

    private Class<?> clazz = null;

    public Methods() {
        super(Method.class);
    }

    @Override
    protected Method generate() {
        if (clazz == null) throw new IllegalStateException("Must use annotation MethodOf");

        return random.choose(clazz.getMethods());
    }

    public void configure(MethodOf methodOf) {
        this.clazz = methodOf.value();
    }

    public static Object[] mockParamsOf(Method method) {
        return Stream.of(method.getParameters()).map(Parameter::getType).map(Methods::mock).toArray();
    }

    private static <T> T mock(Class<T> clazz) {
        if (clazz.equals(boolean.class) || clazz.equals(Object.class)) {
            return (T) Boolean.FALSE;
        } else if (clazz.equals(String.class)) {
            return (T) "";
        } else {
            return Mockito.mock(clazz);
        }
    }

    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface MethodOf {
        Class<?> value();
    }
}
