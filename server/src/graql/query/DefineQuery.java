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

package grakn.core.graql.query;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.query.pattern.Statement;

import java.util.Collection;
import java.util.stream.Collectors;

public class DefineQuery implements Query<ConceptMap> {

    private final Collection<? extends Statement> statements;

    public DefineQuery(Collection<? extends Statement> statements) {
        if (statements == null) {
            throw new NullPointerException("Null statements");
        }
        this.statements = statements;
    }

    public Collection<? extends Statement> statements() {
        return statements;
    }

    @Override
    public String toString() {
        return "define " + statements().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof DefineQuery) {
            DefineQuery that = (DefineQuery) o;
            return this.statements.equals(that.statements());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.statements.hashCode();
        return h;
    }
}
