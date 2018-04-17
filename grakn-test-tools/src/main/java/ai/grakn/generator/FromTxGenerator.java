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

import ai.grakn.GraknTx;
import com.pholser.junit.quickcheck.generator.GeneratorConfiguration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.function.Supplier;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Generator for creating things using an existing tx.
 *
 * @param <T> the type of thing to generate
 *
 * @author Felix Chapman
 */
public abstract class FromTxGenerator<T> extends AbstractGenerator<T> {
    private Supplier<GraknTx> txSupplier =
            () -> gen().make(GraknTxs.class).setOpen(true).generate(random, status);

    private GraknTx tx;

    FromTxGenerator(Class<T> type) {
        super(type);
    }

    protected final GraknTx tx() {
        return tx;
    }

    @Override
    protected final T generate() {
        tx = txSupplier.get();
        return generateFromTx();
    }

    protected abstract T generateFromTx();

    protected final <S extends FromTxGenerator<?>> S genFromTx(Class<S> generatorClass) {
        S generator = gen().make(generatorClass);
        generator.fromTx(this::tx);
        return generator;
    }

    @SuppressWarnings("unused")/** Used through the {@link FromTx} annotation*/
    public final void configure(@SuppressWarnings("unused") FromTx fromTx) {
        fromLastGeneratedTx();
    }

    final FromTxGenerator<T> fromTx(Supplier<GraknTx> txSupplier) {
        this.txSupplier = txSupplier;
        return this;
    }

    final FromTxGenerator<T> fromLastGeneratedTx() {
        fromTx(GraknTxs::lastGeneratedGraph);
        return this;
    }

    /**
     * Specify that the generated objects should be from the {@link GraknTx} generated in a previous parameter
     */
    @Target({PARAMETER, FIELD, ANNOTATION_TYPE, TYPE_USE})
    @Retention(RUNTIME)
    @GeneratorConfiguration
    public @interface FromTx {
    }
}
