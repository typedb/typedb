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

import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;

/**
 * @author Felix Chapman
 */
public class Vars extends AbstractGenerator<Var> {

    public Vars() {
        super(Var.class);
    }

    public Var generate() {
        if (random.nextBoolean() && false) { // TODO
            return Graql.var();
        } else {
            // We use a very limited number of variable names to encourage queries with similar variables
            // This lets us test more interesting queries, improves readability and performance of the tests
            return Graql.var(gen().make(MetasyntacticStrings.class).generate(random, status));
        }
    }
}