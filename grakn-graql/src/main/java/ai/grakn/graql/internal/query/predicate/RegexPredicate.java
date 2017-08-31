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
 */

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.util.Schema;
import ai.grakn.util.StringUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

class RegexPredicate implements ValuePredicate {

    private final String pattern;

    /**
     * @param pattern the regex pattern that this predicate is testing against
     */
    RegexPredicate(String pattern) {
        this.pattern = pattern;
    }

    private P<Object> regexPredicate() {
        BiPredicate<Object, Object> predicate = (value, p) -> Pattern.matches((String) p, (String) value);
        return new P<>(predicate, pattern);
    }

    @Override
    public Optional<P<Object>> getPredicate() {
        return Optional.of(regexPredicate());
    }

    @Override
    public Optional<VarPatternAdmin> getInnerVar() {
        return Optional.empty();
    }

    @Override
    public <S, E> GraphTraversal<S, E> applyPredicate(GraphTraversal<S, E> traversal) {
        return traversal.has(Schema.VertexProperty.VALUE_STRING.name(), regexPredicate());
    }

    @Override
    public String toString() {
        return "/" + StringUtil.escapeString(pattern) + "/";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegexPredicate that = (RegexPredicate) o;

        return pattern != null ? pattern.equals(that.pattern) : that.pattern == null;

    }

    @Override
    public int hashCode() {
        return pattern != null ? pattern.hashCode() : 0;
    }

    @Override
    public boolean isCompatibleWith(ValuePredicate predicate){
        if (!(predicate instanceof EqPredicate)) return false;
        EqPredicate p = (EqPredicate) predicate;
        Object pVal = p.equalsValue().orElse(null);
        return pVal == null || regexPredicate().test(pVal);
    }
}
