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

package grakn.core.concept.type.impl;

import grakn.core.concept.type.Rule;
import grakn.core.graph.Graphs;
import grakn.core.graph.vertex.RuleVertex;
import graql.lang.pattern.Pattern;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static grakn.core.graph.util.Encoding.Edge.Rule.CONCLUSION;
import static grakn.core.graph.util.Encoding.Edge.Rule.CONDITION_NEGATIVE;
import static grakn.core.graph.util.Encoding.Edge.Rule.CONDITION_POSITIVE;

public class RuleImpl implements Rule {

    private final Graphs graphs;
    private final RuleVertex vertex;

    private RuleImpl(Graphs graphs, RuleVertex vertex) {
        this.graphs = graphs;
        this.vertex = vertex;
    }

    private RuleImpl(Graphs graphs, String label, Pattern when, Pattern then) {
        this.graphs = graphs;
        this.vertex = graphs.schema().create(label, when, then);
        putPositiveConditions();
        putNegativeConditions();
        putConclusions();
    }

    public static RuleImpl of(Graphs graphs, RuleVertex vertex) {
        return new RuleImpl(graphs, vertex);
    }

    public static RuleImpl of(Graphs graphs, String label, Pattern when, Pattern then) {
        return new RuleImpl(graphs, label, when, then);
    }

    private void putPositiveConditions() {

        // TODO extract when Types from when pattern
        List<String> whenTypesPositive = Arrays.asList();

        vertex.outs().delete(CONDITION_POSITIVE);

        whenTypesPositive.forEach(label -> {
            vertex.outs().put(CONDITION_POSITIVE, graphs.schema().getRule(label));
        });
    }

    private void putNegativeConditions() {

        // TODO extract when Types from when pattern
        List<String> whenTypesNegative = Arrays.asList();

        vertex.outs().delete(CONDITION_NEGATIVE);

        whenTypesNegative.forEach(label -> {
            vertex.outs().put(CONDITION_NEGATIVE, graphs.schema().getRule(label));
        });
    }

    private void putConclusions() {

        // TODO extract when Types from then pattern
        List<String> conclusionTypes = Arrays.asList();

        vertex.outs().delete(CONCLUSION);

        conclusionTypes.forEach(label -> {
            vertex.outs().put(CONCLUSION, graphs.schema().getRule(label));
        });
    }

    @Override
    public String getLabel() {
        return vertex.label();
    }

    @Override
    public void setLabel(String label) {
        vertex.label(label);
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
        return vertex.outs().edge(CONDITION_POSITIVE).to().apply(v -> TypeImpl.of(graphs, v)).stream();
    }

    @Override
    public Stream<TypeImpl> negativeConditionTypes() {
        return vertex.outs().edge(CONDITION_NEGATIVE).to().apply(v -> TypeImpl.of(graphs, v)).stream();
    }

    @Override
    public Stream<TypeImpl> conclusionTypes() {
        return vertex.outs().edge(CONCLUSION).to().apply(v -> TypeImpl.of(graphs, v)).stream();
    }

    @Override
    public void delete() {
        vertex.delete();
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
