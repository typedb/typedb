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

package grakn.core.graql.query.predicate;

import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.statement.Statement;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

class RegexPredicate implements ValuePredicate {

    private final String pattern;

    RegexPredicate(String pattern) {
        if (pattern == null) {
            throw new NullPointerException("Null pattern");
        }
        this.pattern = pattern;
    }

    String pattern() {
        return pattern;
    }

    private P<Object> regexPredicate() {
        BiPredicate<Object, Object> predicate = (value, p) -> Pattern.matches((String) p, (String) value);
        return new P<>(predicate, pattern());
    }

    @Override
    public Optional<P<Object>> getPredicate() {
        return Optional.of(regexPredicate());
    }

    @Override
    public Optional<Statement> getInnerVar() {
        return Optional.empty();
    }

    @Override
    public <S, E> GraphTraversal<S, E> applyPredicate(GraphTraversal<S, E> traversal) {
        return traversal.has(Schema.VertexProperty.VALUE_STRING.name(), regexPredicate());
    }

    @Override
    public String toString() {
        return Query.Operator.LIKE + " \"" + pattern().replaceAll("/", "\\\\/") + "\"";
    }

    @Override
    public boolean isCompatibleWith(ValuePredicate predicate) {
        if (!(predicate instanceof EqPredicate)) return false;
        EqPredicate p = (EqPredicate) predicate;
        Object thatVal = p.equalsValue().orElse(null);
        return thatVal == null || this.regexPredicate().test(thatVal);
    }

    @Override
    public boolean subsumes(ValuePredicate predicate) {
        return this.isCompatibleWith(predicate);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RegexPredicate) {
            RegexPredicate that = (RegexPredicate) o;
            return (this.pattern.equals(that.pattern()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.pattern.hashCode();
        return h;
    }
}
