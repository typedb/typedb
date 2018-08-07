/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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