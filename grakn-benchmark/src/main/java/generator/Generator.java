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

package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.TypeStrategy;

import java.util.stream.Stream;

/**
 * @param <T>
 */
// TODO Should generator have an interface to make it easy to pass generators of different types. This means passing a TypeStrategy as a parameter
public abstract class Generator<T extends TypeStrategy> implements GeneratorInterface {

    protected final T strategy;
    protected final GraknTx tx;

    /**
     * @param strategy
     * @param tx
     */
    public Generator(T strategy, GraknTx tx) {
        this.strategy = strategy;
        this.tx = tx;
    }

    /**
     * @return
     */
    public abstract Stream<Query> generate();

}
