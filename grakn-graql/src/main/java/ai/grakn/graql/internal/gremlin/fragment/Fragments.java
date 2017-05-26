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
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarProperty;
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
import static ai.grakn.util.Schema.EdgeLabel.SUB;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Factory for creating instances of {@link Fragment}.
 *
 * @author Felix Chapman
 */
public class Fragments {

    private Fragments() {
    }

    public static Fragment inShortcut(VarProperty varProperty,
                                      Var rolePlayer, Var edge, Var relation,
                                      Optional<Set<TypeLabel>> roleTypes, Optional<Set<TypeLabel>> relationTypes) {
        return new InShortcutFragment(varProperty, rolePlayer, edge, relation, roleTypes, relationTypes);
    }

    public static Fragment outShortcut(VarProperty varProperty,
                                       Var relation, Var edge, Var rolePlayer,
                                       Optional<Set<TypeLabel>> roleTypes, Optional<Set<TypeLabel>> relationTypes) {
        return new OutShortcutFragment(varProperty, relation, edge, rolePlayer, roleTypes, relationTypes);
    }

    public static Fragment inSub(VarProperty varProperty, Var start, Var end) {
        return new InSubFragment(varProperty, start, end);
    }

    public static Fragment outSub(VarProperty varProperty, Var start, Var end) {
        return new OutSubFragment(varProperty, start, end);
    }

    public static InRelatesFragment inRelates(VarProperty varProperty, Var start, Var end) {
        return new InRelatesFragment(varProperty, start, end);
    }

    public static Fragment outRelates(VarProperty varProperty, Var start, Var end) {
        return new OutRelatesFragment(varProperty, start, end);
    }

    public static Fragment inIsa(VarProperty varProperty, Var start, Var end) {
        return new InIsaFragment(varProperty, start, end, false);
    }

    public static Fragment outIsa(VarProperty varProperty, Var start, Var end) {
        return new OutIsaFragment(varProperty, start, end, false);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static Fragment inIsaCastings(VarProperty varProperty, Var start, Var end) {
        return new InIsaFragment(varProperty, start, end, true);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static Fragment outIsaCastings(VarProperty varProperty, Var start, Var end) {
        return new OutIsaFragment(varProperty, start, end, true);
    }

    public static Fragment inHasScope(VarProperty varProperty, Var start, Var end) {
        return new InHasScopeFragment(varProperty, start, end);
    }

    public static Fragment outHasScope(VarProperty varProperty, Var start, Var end) {
        return new OutHasScopeFragment(varProperty, start, end);
    }

    public static Fragment dataType(VarProperty varProperty, Var start, ResourceType.DataType dataType) {
        return new DataTypeFragment(varProperty, start, dataType);
    }

    public static Fragment inPlays(VarProperty varProperty, Var start, Var end, boolean required) {
        return new InPlaysFragment(varProperty, start, end, required);
    }

    public static Fragment outPlays(VarProperty varProperty, Var start, Var end, boolean required) {
        return new OutPlaysFragment(varProperty, start, end, required);
    }

    public static Fragment inCasting(VarProperty varProperty, Var start, Var end) {
        return new InCastingFragment(varProperty, start, end);
    }

    public static Fragment outCasting(VarProperty varProperty, Var start, Var end) {
        return new OutCastingFragment(varProperty, start, end);
    }

    public static Fragment inRolePlayer(VarProperty varProperty, Var start, Var end) {
        return new InRolePlayerFragment(varProperty, start, end);
    }

    public static Fragment outRolePlayer(VarProperty varProperty, Var start, Var end) {
        return new OutRolePlayerFragment(varProperty, start, end);
    }

    public static Fragment id(VarProperty varProperty, Var start, ConceptId id) {
        return new IdFragment(varProperty, start, id);
    }

    public static Fragment label(VarProperty varProperty, Var start, TypeLabel label) {
        return new LabelFragment(varProperty, start, label);
    }

    public static Fragment value(VarProperty varProperty, Var start, ValuePredicateAdmin predicate) {
        return new ValueFragment(varProperty, start, predicate);
    }

    public static Fragment isAbstract(VarProperty varProperty, Var start) {
        return new IsAbstractFragment(varProperty, start);
    }

    public static Fragment regex(VarProperty varProperty, Var start, String regex) {
        return new RegexFragment(varProperty, start, regex);
    }

    public static Fragment notInternal(VarProperty varProperty, Var start) {
        return new NotInternalFragment(varProperty, start);
    }

    public static Fragment neq(VarProperty varProperty, Var start, Var other) {
        return new NeqFragment(varProperty, start, other);
    }

    /**
     * A {@link Fragment} that uses an index stored on each resource. Resources are indexed by direct type and value.
     */
    public static Fragment resourceIndex(VarProperty varProperty, Var start, TypeLabel typeLabel, Object resourceValue) {
        return new ResourceIndexFragment(varProperty, start, typeLabel, resourceValue);
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

    static String displayOptionalTypeLabels(Optional<Set<TypeLabel>> typeLabels) {
        return typeLabels.map(labels ->
                " " + labels.stream().map(StringConverter::typeLabelToString).collect(joining(","))
        ).orElse("");
    }

    static void applyTypeLabelsToTraversal(
            GraphTraversal<Vertex, Edge> traversal, Schema.EdgeProperty property, Optional<Set<TypeLabel>> typeLabels, GraknGraph graph) {
        typeLabels.ifPresent(labels -> {
            Set<Integer> typeIds = labels.stream().map(label -> graph.admin().convertToId(label).getValue()).collect(toSet());
            traversal.has(property.name(), P.within(typeIds));
        });
    }
}
