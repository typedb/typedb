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

package grakn.core.graql.query.pattern;

import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.property.VarProperty;
import java.util.Collections;
import java.util.Set;

/**
 *
 */
public class PositiveStatement extends Statement {

    public PositiveStatement(Variable var) { super(var); }
    public PositiveStatement(Variable var, Set<VarProperty> properties) { super(var, properties); }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<Statement> conjunction = Graql.and(Collections.singleton(this));
        return Graql.or(Collections.singleton(conjunction));
    }

    @Override
    public NegativeStatement negate(){
        return new NegativeStatement(var(), properties());
    }
}
