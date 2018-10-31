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

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.util.Schema;
import com.google.auto.value.AutoValue;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

@AutoValue
abstract class RegexPredicate implements ValuePredicate {

    abstract String pattern();

    /**
     * @param pattern the regex pattern that this predicate is testing against
     */
    static RegexPredicate of(String pattern) {
        return new AutoValue_RegexPredicate(pattern);
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
    public Optional<VarPatternAdmin> getInnerVar() {
        return Optional.empty();
    }

    @Override
    public <S, E> GraphTraversal<S, E> applyPredicate(GraphTraversal<S, E> traversal) {
        return traversal.has(Schema.VertexProperty.VALUE_STRING.name(), regexPredicate());
    }

    @Override
    public String toString() {
        return "/" + pattern().replaceAll("/", "\\\\/") + "/";
    }

    @Override
    public boolean isCompatibleWith(ValuePredicate predicate){
        if (!(predicate instanceof EqPredicate)) return false;
        EqPredicate p = (EqPredicate) predicate;
        Object thatVal = p.equalsValue().orElse(null);
        return thatVal == null || this.regexPredicate().test(thatVal);
    }
}
