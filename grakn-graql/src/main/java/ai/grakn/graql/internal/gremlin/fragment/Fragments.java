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

package ai.grakn.graql.internal.gremlin.fragment;

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

import javax.annotation.Nullable;
import java.util.Set;

import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static ai.grakn.util.Schema.VertexProperty.LABEL_ID;
import static ai.grakn.util.Schema.VertexProperty.THING_TYPE_LABEL_ID;
import static java.util.stream.Collectors.joining;

/**
 * Factory for creating instances of {@link Fragment}.
 *
 * @author Felix Chapman
 */
public class Fragments {

    private Fragments() {
    }

    public static Fragment inRolePlayer(VarProperty varProperty,
                                        Var rolePlayer, Var edge, Var relation, @Nullable Var role,
                                        @Nullable ImmutableSet<Label> roleLabels,
                                        @Nullable ImmutableSet<Label> relationTypeLabels) {
        return new AutoValue_InRolePlayerFragment(
                varProperty, rolePlayer, relation, edge, role, roleLabels, relationTypeLabels);
    }

    public static Fragment outRolePlayer(VarProperty varProperty,
                                         Var relation, Var edge, Var rolePlayer, @Nullable Var role,
                                         @Nullable ImmutableSet<Label> roleLabels,
                                         @Nullable ImmutableSet<Label> relationTypeLabels) {
        return new AutoValue_OutRolePlayerFragment(
                varProperty, relation, rolePlayer, edge, role, roleLabels, relationTypeLabels);
    }

    public static Fragment inSub(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_InSubRepeatedFragment(varProperty, start, end);
    }

    public static Fragment outSub(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_OutSubRepeatedFragment(varProperty, start, end);
    }

    public static Fragment inSubLimited(VarProperty varProperty, Var start, Var end, int limit) {
        return new AutoValue_InSubLimitedFragment(varProperty, start, end, limit);
    }

    public static Fragment outSubLimited(VarProperty varProperty, Var start, Var end, int limit) {
        return new AutoValue_OutSubLimitedFragment(varProperty, start, end, limit);
    }

    public static InRelatesFragment inRelates(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_InRelatesFragment(varProperty, start, end);
    }

    public static Fragment outRelates(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_OutRelatesFragment(varProperty, start, end);
    }

    public static Fragment inIsa(VarProperty varProperty, Var start, Var end, boolean mayHaveEdgeInstances) {
        return new AutoValue_InIsaFragment(varProperty, start, end, mayHaveEdgeInstances);
    }

    public static Fragment outIsa(VarProperty varProperty, Var start, Var end) {
        return new AutoValue_OutIsaFragment(varProperty, start, end);
    }

    public static Fragment dataType(VarProperty varProperty, Var start, AttributeType.DataType dataType) {
        return new AutoValue_DataTypeFragment(varProperty, start, dataType);
    }

    public static Fragment inPlays(VarProperty varProperty, Var start, Var end, boolean required) {
        return new AutoValue_InPlaysFragment(varProperty, start, end, required);
    }

    public static Fragment outPlays(VarProperty varProperty, Var start, Var end, boolean required) {
        return new AutoValue_OutPlaysFragment(varProperty, start, end, required);
    }

    public static Fragment id(VarProperty varProperty, Var start, ConceptId id) {
        return new AutoValue_IdFragment(varProperty, start, id);
    }

    public static Fragment label(VarProperty varProperty, Var start, ImmutableSet<Label> labels) {
        return new AutoValue_LabelFragment(varProperty, start, labels);
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
     * A {@link Fragment} that uses an index stored on each attribute. Attributes are indexed by direct type and value.
     */
    public static Fragment attributeIndex(
            @Nullable VarProperty varProperty, Var start, Label label, Object attributeValue) {
        String attributeIndex = Schema.generateAttributeIndex(label, attributeValue.toString());
        return new AutoValue_AttributeIndexFragment(varProperty, start, attributeIndex);
    }

    static <T> GraphTraversal<T, Vertex> outSubs(GraphTraversal<T, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `THING_TYPE_LABEL_ID` property
        return union(traversal, ImmutableSet.of(
                __.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name()))
                ,
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

    static <T> GraphTraversal<T, Vertex> outSubsLimited(GraphTraversal<T, Vertex> traversal, int subSteps) {
        // These traversals make sure to only navigate types by checking they do not have a `THING_TYPE_LABEL_ID` property
        return union(traversal, ImmutableSet.of(
                __.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name()))
                ,
                __.repeat(__.out(SUB.getLabel())).times(subSteps).emit()
        )).unfold();
    }

    static <T> GraphTraversal<T, Vertex> inSubsLimited(GraphTraversal<T, Vertex> traversal, int subSteps) {
        // These traversals make sure to only navigate types by checking they do not have a `THING_TYPE_LABEL_ID` property
        return union(traversal, ImmutableSet.of(
                __.<Vertex>not(__.has(THING_TYPE_LABEL_ID.name())).not(__.hasLabel(Schema.BaseType.SHARD.name())),
                __.repeat(__.in(SUB.getLabel())).times(subSteps).emit()
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

    static String displayOptionalTypeLabels(String name, @Nullable Set<Label> typeLabels) {
        if (typeLabels != null) {
            return " " + name + ":" + typeLabels.stream().map(StringConverter::typeLabelToString).collect(joining(","));
        } else {
            return "";
        }
    }

    static <S> GraphTraversal<S, Vertex> traverseSchemaConceptFromEdge(
            GraphTraversal<S, Edge> traversal, Schema.EdgeProperty edgeProperty) {

        // Access label ID from edge
        Var labelId = Graql.var();
        traversal.values(edgeProperty.name()).as(labelId.name());

        // Look up schema concept using ID
        return traversal.V().has(LABEL_ID.name(), __.where(P.eq(labelId.name())));
    }

}
