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

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.internal.util.StringConverter;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;

import static ai.grakn.util.Schema.ConceptProperty.INSTANCE_TYPE_ID;
import static ai.grakn.util.Schema.ConceptProperty.TYPE_ID;
import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static ai.grakn.util.Schema.EdgeProperty.ROLE_TYPE_ID;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Factory for creating instances of {@link Fragment}.
 *
 * @author Felix Chapman
 */
public class Fragments {

    private Fragments() {}

    public static Fragment inShortcut(
            Var rolePlayer, Var edge, Var relation, Optional<Var> roleType,
            Optional<Set<TypeLabel>> roleTypeLabels, Optional<Set<TypeLabel>> relationTypeLabels) {
        return new InShortcutFragment(rolePlayer, edge, relation, roleType, roleTypeLabels, relationTypeLabels);
    }

    public static Fragment outShortcut(
            Var relation, Var edge, Var rolePlayer, Optional<Var> roleType,
            Optional<Set<TypeLabel>> roleTypeLabels, Optional<Set<TypeLabel>> relationTypeLabels) {
        return new OutShortcutFragment(relation, edge, rolePlayer, roleType, roleTypeLabels, relationTypeLabels);
    }

    public static Fragment inSub(Var start, Var end) {
        return new InSubFragment(start, end);
    }

    public static Fragment outSub(Var start, Var end) {
        return new OutSubFragment(start, end);
    }

    public static InRelatesFragment inRelates(Var start, Var end) {
        return new InRelatesFragment(start, end);
    }

    public static Fragment outRelates(Var start, Var end) {
        return new OutRelatesFragment(start, end);
    }

    public static Fragment inIsa(Var start, Var end) {
        return new InIsaFragment(start, end);
    }

    public static Fragment outIsa(Var start, Var end) {
        return new OutIsaFragment(start, end);
    }

    public static Fragment inHasScope(Var start, Var end) {
        return new InHasScopeFragment(start, end);
    }

    public static Fragment outHasScope(Var start, Var end) {
        return new OutHasScopeFragment(start, end);
    }

    public static Fragment dataType(Var start, ResourceType.DataType dataType) {
        return new DataTypeFragment(start, dataType);
    }

    public static Fragment inPlays(Var start, Var end, boolean required) {
        return new InPlaysFragment(start, end, required);
    }

    public static Fragment outPlays(Var start, Var end, boolean required) {
        return new OutPlaysFragment(start, end, required);
    }

    public static Fragment id(Var start, ConceptId id) {
        return new IdFragment(start, id);
    }

    public static Fragment label(Var start, TypeLabel label) {
        return new LabelFragment(start, label);
    }

    public static Fragment value(Var start, ValuePredicateAdmin predicate) {
        return new ValueFragment(start, predicate);
    }

    public static Fragment isAbstract(Var start) {
        return new IsAbstractFragment(start);
    }

    public static Fragment regex(Var start, String regex) {
        return new RegexFragment(start, regex);
    }

    public static Fragment notInternal(Var start) {
        return new NotInternalFragment(start);
    }

    public static Fragment neq(Var start, Var other) {
        return new NeqFragment(start, other);
    }

    /**
     * A {@link Fragment} that uses an index stored on each resource. Resources are indexed by direct type and value.
     */
    public static Fragment resourceIndex(Var start, TypeLabel typeLabel, Object resourceValue) {
        return new ResourceIndexFragment(start, typeLabel, resourceValue);
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> outSubs(GraphTraversal<Vertex, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `INSTANCE_TYPE_ID` property
        return traversal.union(__.not(__.has(INSTANCE_TYPE_ID.name())), __.repeat(__.out(SUB.getLabel())).emit()).unfold();
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> inSubs(GraphTraversal<Vertex, Vertex> traversal) {
        // These traversals make sure to only navigate types by checking they do not have a `INSTANCE_TYPE_ID` property
        return traversal.union(__.not(__.has(INSTANCE_TYPE_ID.name())), __.repeat(__.in(SUB.getLabel())).emit()).unfold();
    }

    static String displayOptionalTypeLabels(String name, Optional<Set<TypeLabel>> typeLabels) {
        return typeLabels.map(labels ->
            " " + name + ":" + labels.stream().map(StringConverter::typeLabelToString).collect(joining(","))
        ).orElse("");
    }

    static void applyTypeLabelsToTraversal(
            GraphTraversal<Vertex, Edge> traversal, Schema.EdgeProperty property, Optional<Set<TypeLabel>> typeLabels, GraknGraph graph) {
        typeLabels.ifPresent(labels -> {
            Set<Integer> typeIds = labels.stream().map(label -> graph.admin().convertToId(label).getValue()).collect(toSet());
            traversal.has(property.name(), P.within(typeIds));
        });
    }

    /**
     * Optionally traverse from a shortcut edge to the role-type it mentions, plus any super-types.
     *
     * @param traversal the traversal, starting from the shortcut edge
     * @param roleType the variable to assign to the role-type. If not present, do nothing
     */
    static void traverseRoleTypeFromShortcutEdge(GraphTraversal<Vertex, Edge> traversal, Optional<Var> roleType) {
        roleType.ifPresent(var -> {
            // Access role-type ID from edge
            Var roleTypeIdProperty = Graql.var();
            Var edge = Graql.var();
            traversal.as(edge.getValue()).values(ROLE_TYPE_ID.name()).as(roleTypeIdProperty.getValue());

            // Look up direct role-type using ID
            GraphTraversal<Vertex, Vertex> vertexTraversal =
                    traversal.V().has(TYPE_ID.name(), __.where(P.eq(roleTypeIdProperty.getValue())));

            // Navigate up type hierarchy
            Fragments.outSubs(vertexTraversal).as(var.getValue());

            traversal.select(edge.getValue());
        });
    }

}
