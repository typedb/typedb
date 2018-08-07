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

import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;

/**
 * @author Felix Chapman
 */
public class Conjunctions extends RecursiveGenerator<Conjunction> {

    public Conjunctions() {
        super(Conjunction.class);
    }

    @Override
    protected Conjunction generateBase() {
        return Patterns.conjunction(setOf(VarPatternAdmin.class, 1, 1));
    }

    @Override
    protected Conjunction generateRecurse() {
        // This is done to favour generating `VarPattern`s instead of endless `Conjunction`s or `Disjunction`s
        if (random.nextFloat() > 0.9) {
            return Patterns.conjunction(setOf(PatternAdmin.class, 1, 3));
        } else {
            return generateBase();
        }
    }
}