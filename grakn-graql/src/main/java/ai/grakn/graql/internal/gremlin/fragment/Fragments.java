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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static ai.grakn.util.Schema.VertexProperty.LABEL_ID;
import static ai.grakn.util.Schema.VertexProperty.THING_TYPE_LABEL_ID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Factory for creating instances of {@link Fragment}.
 *
 * @author Felix Chapman
 */
public class Fragments {

    // TODO: Make sure these never clash with a real Graql variable
    static final String RELATION_EDGE = "!RELATION_EDGE";
    static final String RELATION_DIRECTION = "!RELATION_DIRECTION";

    private Fragments() {
    }

    public static Fragment inShortcut(VarProperty varProperty,
                                      Var rolePlayer, Var edge, Var relation, Optional<Var> role,
                                      Optional<Set<Label>> roleLabels, Optional<Set<Label>> relationTypeLabels) {
        return new AutoValue_InShortcutFragment(
                varProperty, rolePlayer, Optional.of(relation), edge, role, roleLabels, relationTypeLabels);
    }

    public static Fragment outShortcut(VarProperty varProperty,
                                       Var relation, Var edge, Var rolePlayer, Optional<Var> role,
                                       Optional<Set<Label>> roleLabels, Optional<Set<Label>> relationTypeLabels) {
        return new AutoValue_OutShortcutFragment(
                varProperty, relation, Optional.of(rolePlayer), edge, role, roleLabels, relationTypeLabels);
    }

    public static Fragment inSub(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_InSubFragment(varProperty, start, Optional.of(end));
    }

    public static Fragment outSub(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_OutSubFragment(varProperty, start, Optional.of(end));
    }

    public static InRelatesFragment inRelates(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_InRelatesFragment(varProperty, start, Optional.of(end));
    }

    public static Fragment outRelates(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_OutRelatesFragment(varProperty, start, Optional.of(end));
    }

    public static Fragment inIsa(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_InIsaFragment(varProperty, start, Optional.of(end));
    }

    public static Fragment outIsa(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_OutIsaFragment(varProperty, start, Optional.of(end));
    }

    public static Fragment dataType(VarProperty varProperty, Var start, AttributeType.DataType dataType) {
        return new AutoValue_DataTypeFragment(varProperty, start, dataType);
    }

    public static Fragment inPlays(VarProperty varProperty, Var start, Var end, boolean required) {
        return new AutoValue_InPlaysFragment(varProperty, start, Optional.of(end), required);
    }

    public static Fragment outPlays(VarProperty varProperty, Var start, Var end, boolean required) {
        return new AutoValue_OutPlaysFragment(varProperty, start, Optional.of(end), required);
    }

    public static Fragment id(VarProperty varProperty, Var start, ConceptId id) {
        return new AutoValue_IdFragment(varProperty, start, id);
    }

    public static Fragment label(VarProperty varProperty, Var start, Label label) {
        return new AutoValue_LabelFragment(varProperty, start, label);
    }

    public static Fragment value(VarProperty varProperty, Var start, ValuePredicate predicate) {
        return new AutoValue_ValueFragment(varProperty, start, predicate);
    }

    public static Fragment isAbstract(VarProperty varProperty, Var start) {
        return new AutoValue_IsAbstractFragment(varProperty, start);
    }

    public static Fragment regex(VarProperty varProperty, Var start, String regex) {
        return new AutoValue_RegexFragment(varProperty, start, regex);
    }

    public static Fragment notInternal(VarProperty varProperty, Var start) {
        return new AutoValue_NotInternalFragment(varProperty, start);
    }

    public static Fragment neq(VarProperty varProperty, Var start, Var other) {
        return new AutoValue_NeqFragment(varProperty, start, other);
    }

    /**
     * A {@link Fragment} that uses an index stored on each resource. Resources are indexed by direct type and value.
     */
    public static Fragment resourceIndex(VarProperty varProperty, Var start, Label label, Object resourceValue) {
        String resourceIndex = Schema.generateAttributeIndex(label, resourceValue.toString());
        return new AutoValue_ResourceIndexFragment(varProperty, start, resourceIndex);
    }

    static <T> GraphTraversal<T, Vertex> outSubs(GraphTraversal<T, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `THING_TYPE_LABEL_ID` property
        return union(traversal, ImmutableSet.of(
                __.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name())),
                __.repeat(__.out(SUB.getLabel())).emit()
        )).unfold();
    }

    static <T> GraphTraversal<T, Vertex> inSubs(GraphTraversal<T, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `THING_TYPE_LABEL_ID` property
        return union(traversal, ImmutableSet.of(
                __.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name())),
                __.repeat(__.in(SUB.getLabel())).emit()
        )).unfold();
    }

    /**
     * A type-safe way to do `__.union(a, b)`, as `Fragments.union(ImmutableSet.of(a, b))`.
     * This avoids issues with unchecked varargs.
     */
    static <S, E> GraphTraversal<S, E> union(Iterable<GraphTraversal<? super S, ? extends E>> traversals) {
        return union(__.identity(), traversals);
    }

    /**
     * A type-safe way to do `a.union(b, c)`, as `Fragments.union(a, ImmutableSet.of(b, c))`.
     * This avoids issues with unchecked varargs.
     */
    static <S, E1, E2> GraphTraversal<S, E2> union(
            GraphTraversal<S, ? extends E1> start, Iterable<GraphTraversal<? super E1, ? extends E2>> traversals) {
        // This is safe, because we know all the arguments are of the right type
        //noinspection unchecked
        GraphTraversal<E1, E2>[] array = (GraphTraversal<E1, E2>[]) Iterables.toArray(traversals, GraphTraversal.class);

        return start.union(array);
    }

    /**
     * Create a traversal that filters to only vertices
     */
    static <T> GraphTraversal<T, Vertex> isVertex(GraphTraversal<T, ? extends Element> traversal) {
        // This cast is safe because we filter only to vertices
        //noinspection unchecked
        return (GraphTraversal<T, Vertex>) traversal.has(Schema.VertexProperty.ID.name());
    }

    /**
     * Create a traversal that filters to only edges
     */
    static <T> GraphTraversal<T, Edge> isEdge(GraphTraversal<T, ? extends Element> traversal) {
        // This cast is safe because we filter only to edges
        //noinspection unchecked
        return (GraphTraversal<T, Edge>) traversal.hasNot(Schema.VertexProperty.ID.name());
    }

    static String displayOptionalTypeLabels(String name, Optional<Set<Label>> typeLabels) {
        return typeLabels.map(labels ->
                " " + name + ":" + labels.stream().map(StringConverter::typeLabelToString).collect(joining(","))
        ).orElse("");
    }

    static void applyTypeLabelsToTraversal(
            GraphTraversal<?, Edge> traversal, Schema.EdgeProperty property, Optional<Set<Label>> typeLabels, GraknTx graph) {
        typeLabels.ifPresent(labels -> {
            Set<Integer> typeIds = labels.stream().map(label -> graph.admin().convertToId(label).getValue()).collect(toSet());
            traversal.has(property.name(), P.within(typeIds));
        });
    }

    /**
     * Optionally traverse from a shortcut edge to the role-type it mentions, plus any super-types.
     *
     * @param traversal the traversal, starting from the shortcut edge
     * @param role the variable to assign to the role. If not present, do nothing
     * @param edgeProperty the edge property to look up the role label ID
     */
    static void traverseRoleFromShortcutEdge(GraphTraversal<?, Edge> traversal, Optional<Var> role, Schema.EdgeProperty edgeProperty) {
        role.ifPresent(var -> {
            Var edge = Graql.var();
            traversal.as(edge.getValue());
            Fragments.outSubs(traverseSchemaConceptFromEdge(traversal, edgeProperty));
            traversal.as(var.getValue()).select(edge.getValue());
        });
    }

    static <S> GraphTraversal<S, Vertex> traverseSchemaConceptFromEdge(
            GraphTraversal<S, Edge> traversal, Schema.EdgeProperty edgeProperty) {

        // Access label ID from edge
        Var labelId = Graql.var();
        traversal.values(edgeProperty.name()).as(labelId.getValue());

        // Look up schema concept using ID
        return traversal.V().has(LABEL_ID.name(), __.where(P.eq(labelId.getValue())));
    }

}
