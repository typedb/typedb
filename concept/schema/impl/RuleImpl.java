/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 *
 */

package grakn.core.concept.schema.impl;

import grakn.core.concept.schema.Rule;
import grakn.core.graph.SchemaGraph;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.RuleVertex;
import graql.lang.pattern.Pattern;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static grakn.core.common.iterator.Iterators.apply;
import static grakn.core.common.iterator.Iterators.stream;

public class RuleImpl implements Rule {

    private final RuleVertex vertex;

    private RuleImpl(RuleVertex vertex) {
        this.vertex = vertex;
    }

    private RuleImpl(SchemaGraph graph, String label, Pattern when, Pattern then) {
        this.vertex = graph.create(label, when, then);
        putPositiveConditions();
        putNegativeConditions();
        putConclusions();
    }

    public static RuleImpl of(RuleVertex vertex) {
        return new RuleImpl(vertex);
    }

    public static RuleImpl of(SchemaGraph graph, String label, Pattern when, Pattern then) {
        return new RuleImpl(graph, label, when, then);
    }

    private void putPositiveConditions() {

        // TODO extract when Types from when pattern
        List<String> whenTypesPositive = Arrays.asList();

        vertex.outs().delete(Encoding.Edge.Rule.CONDITION_POSITIVE);

        whenTypesPositive.forEach(label -> {
            vertex.outs().put(Encoding.Edge.Rule.CONDITION_POSITIVE, vertex.graph().getRule(label));
        });
    }

    private void putNegativeConditions() {

        // TODO extract when Types from when pattern
        List<String> whenTypesNegative = Arrays.asList();

        vertex.outs().delete(Encoding.Edge.Rule.CONDITION_NEGATIVE);

        whenTypesNegative.forEach(label -> {
            vertex.outs().put(Encoding.Edge.Rule.CONDITION_NEGATIVE, vertex.graph().getRule(label));
        });
    }

    private void putConclusions() {

        // TODO extract when Types from then pattern
        List<String> conclusionTypes = Arrays.asList();

        vertex.outs().delete(Encoding.Edge.Rule.CONCLUSION);

        conclusionTypes.forEach(label -> {
            vertex.outs().put(Encoding.Edge.Rule.CONCLUSION, vertex.graph().getRule(label));
        });
    }

    @Override
    public Pattern getWhen() {
        return vertex.when();
    }

    @Override
    public Pattern getThen() {
        return vertex.then();
    }

    @Override
    public void setWhen(Pattern when) {
        vertex.when(when);
    }

    @Override
    public void setThen(Pattern then) {
        vertex.then(then);
    }

    @Override
    public Stream<TypeImpl> positiveConditionTypes() {
        return stream(apply(vertex.outs().edge(Encoding.Edge.Rule.CONDITION_POSITIVE).to(), TypeImpl::of));
    }

    @Override
    public Stream<TypeImpl> negativeConditionTypes() {
        return stream(apply(vertex.outs().edge(Encoding.Edge.Rule.CONDITION_NEGATIVE).to(), TypeImpl::of));
    }

    @Override
    public Stream<TypeImpl> conclusionTypes() {
        return stream(apply(vertex.outs().edge(Encoding.Edge.Rule.CONCLUSION).to(), TypeImpl::of));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        RuleImpl that = (RuleImpl) object;
        return this.vertex.equals(that.vertex);
    }

    @Override
    public final int hashCode() {
        return vertex.hashCode(); // does not need caching
    }
}
