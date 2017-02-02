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

import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;

public class PatternGenerator extends AbstractGenerator<Pattern> {

    public PatternGenerator() {
        super(Pattern.class);
    }

    @Override
    protected Pattern generate() {
        // TODO: Make this produce conjunctions and disjunctions
        return gen(Var.class);
        // return gen().oneOf(Disjunction.class, Conjunction.class, Var.class).generate(random, status);
    }
}
