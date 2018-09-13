/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package generator;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;
import strategy.RolePlayerTypeStrategy;

import java.util.stream.Stream;

/**
 *
 */
public class RolePlayerGenerator extends Generator<RolePlayerTypeStrategy> {

    /**
     * @param strategy
     * @param tx
     */
    public RolePlayerGenerator(RolePlayerTypeStrategy strategy, GraknTx tx) {
        super(strategy, tx);
    }

    /**
     * @return
     */
    @Override
    public Stream<Query> generate() {
        return null;
    }
}
