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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.generator;

import ai.grakn.concept.Type;
import com.google.common.base.Preconditions;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Optional;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Abstract class for generating {@link Type}s.
 *
 * @param <T> the kind of {@link Type} to generate
 *
 * @author Felix Chapman
 */
public abstract class AbstractTypeGenerator<T extends Type> extends AbstractSchemaConceptGenerator<T> {

    private Optional<Boolean> includeAbstract = Optional.empty();

    AbstractTypeGenerator(Class<T> type) {
        super(type);
    }

    private boolean willIncludeAbstractTypes(){
        return includeAbstract.orElse(true);
    }

    final AbstractSchemaConceptGenerator<T> makeExcludeAbstractTypes() {
        includeAbstract = Optional.of(false);
        return this;
    }

    @SuppressWarnings("unused") /**Used through annotation {@link Abstract}*/
    public final void configure(@SuppressWarnings("unused") Abstract abstract_) {
        Preconditions.checkArgument(
                !this.includeAbstract.isPresent() || this.includeAbstract.get(),
                "Cannot specify parameter is both abstract and non-abstract"
        );
        this.includeAbstract = Optional.of(true);
    }

    @SuppressWarnings("unused") /**Used through annotation {@link Abstract}*/
    public final void configure(@SuppressWarnings("unused") NonAbstract nonAbstract) {
        Preconditions.checkArgument(
                !this.includeAbstract.isPresent() || !this.includeAbstract.get(),
                "Cannot specify parameter is both abstract and non-abstract"
        );
        this.includeAbstract = Optional.of(false);
    }

    @Override
    protected final boolean filter(T schemaConcept) {
        return willIncludeAbstractTypes() || !schemaConcept.isAbstract();
    }

    /**
     * Specify that the generated {@link Type} should be abstract
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface Abstract {
    }

    /**
     * Specify that the generated {@link Type} should not be abstract
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface NonAbstract {
    }
}
