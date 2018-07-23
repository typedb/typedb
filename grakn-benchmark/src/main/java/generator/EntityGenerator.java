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
import ai.grakn.graql.QueryBuilder;
import strategy.EntityStrategy;

import java.util.stream.Stream;

import static ai.grakn.graql.internal.pattern.Patterns.var;

/**
 *
 */
public class EntityGenerator extends Generator<EntityStrategy> {
    public EntityGenerator(EntityStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    /**
     * @return
     */
    @Override
    public Stream<Query> generate() {
        QueryBuilder qb = this.tx.graql();

        // TODO Can using toString be avoided? Waiting for TP task #20179
//        String entityTypeName = this.strategy.getType().label().getValue();

        String typeLabel = this.strategy.getTypeLabel();
        Query query = qb.insert(var("x").isa(typeLabel));

        int numInstances = this.strategy.getNumInstancesPDF().next();

        Stream<Query> stream = Stream.generate(() -> query)
//                .map(q -> (Query) q)
                .limit(numInstances);
        return stream;
    }
}