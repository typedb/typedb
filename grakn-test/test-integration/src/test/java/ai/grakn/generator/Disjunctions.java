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

import ai.grakn.graql.admin.Disjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;

/**
 * @author Felix Chapman
 */
public class Disjunctions extends RecursiveGenerator<Disjunction> {

    public Disjunctions() {
        super(Disjunction.class);
    }

    @Override
    protected Disjunction generateBase() {
        return Patterns.disjunction(setOf(VarPatternAdmin.class, 1, 1));
    }

    @Override
    protected Disjunction generateRecurse() {
        // This is done to favour generating `VarPattern`s instead of endless `Conjunction`s or `Disjunction`s
        if (random.nextFloat() > 0.9) {
            return Patterns.disjunction(setOf(PatternAdmin.class, 1, 3));
        } else {
            return generateBase();
        }
    }
}