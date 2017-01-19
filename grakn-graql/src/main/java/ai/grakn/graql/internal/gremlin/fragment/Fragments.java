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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;

import static ai.grakn.util.Schema.EdgeLabel.SUB;

public class Fragments {

    private Fragments() {}

    public static Fragment shortcut(
            Optional<TypeName> relationType, Optional<TypeName> roleStart, Optional<TypeName> roleEnd,
            VarName start, VarName end
    ) {
        return new ShortcutFragment(relationType, roleStart, roleEnd, start, end);
    }

    public static Fragment inSub(VarName start, VarName end) {
        return new InSubFragment(start, end);
    }

    public static Fragment outSub(VarName start, VarName end) {
        return new OutSubFragment(start, end);
    }

    public static InHasRoleFragment inHasRole(VarName start, VarName end) {
        return new InHasRoleFragment(start, end);
    }

    public static Fragment outHasRole(VarName start, VarName end) {
        return new OutHasRoleFragment(start, end);
    }

    public static InIsaFragment inIsa(VarName start, VarName end) {
        return new InIsaFragment(start, end, false);
    }

    public static OutIsaFragment outIsa(VarName start, VarName end) {
        return new OutIsaFragment(start, end, false);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static InIsaFragment inIsaCastings(VarName start, VarName end) {
        return new InIsaFragment(start, end, true);
    }

    // This method is a special case that allows getting the instances of role-types (castings)
    public static OutIsaFragment outIsaCastings(VarName start, VarName end) {
        return new OutIsaFragment(start, end, true);
    }

    public static InHasScopeFragment inHasScope(VarName start, VarName end) {
        return new InHasScopeFragment(start, end);
    }

    public static OutHasScopeFragment outHasScope(VarName start, VarName end) {
        return new OutHasScopeFragment(start, end);
    }

    public static DataTypeFragment dataType(VarName start, ResourceType.DataType dataType) {
        return new DataTypeFragment(start, dataType);
    }

    public static InPlaysRoleFragment inPlaysRole(VarName start, VarName end, boolean required) {
        return new InPlaysRoleFragment(start, end, required);
    }

    public static OutPlaysRoleFragment outPlaysRole(VarName start, VarName end, boolean required) {
        return new OutPlaysRoleFragment(start, end, required);
    }

    public static InCastingFragment inCasting(VarName start, VarName end) {
        return new InCastingFragment(start, end);
    }

    public static OutCastingFragment outCasting(VarName start, VarName end) {
        return new OutCastingFragment(start, end);
    }

    public static InRolePlayerFragment inRolePlayer(VarName start, VarName end) {
        return new InRolePlayerFragment(start, end);
    }

    public static OutRolePlayerFragment outRolePlayer(VarName start, VarName end) {
        return new OutRolePlayerFragment(start, end);
    }

    public static DistinctCastingFragment distinctCasting(VarName start, VarName otherCastingName) {
        return new DistinctCastingFragment(start, otherCastingName);
    }

    public static IdFragment id(VarName start, ConceptId id) {
        return new IdFragment(start, id);
    }

    public static Fragment name(VarName start, TypeName name) {
        return new NameFragment(start, name);
    }

    public static ValueFragment value(VarName start, ValuePredicateAdmin predicate) {
        return new ValueFragment(start, predicate);
    }

    public static IsAbstractFragment isAbstract(VarName start) {
        return new IsAbstractFragment(start);
    }

    public static RegexFragment regex(VarName start, String regex) {
        return new RegexFragment(start, regex);
    }

    public static ValueFlagFragment value(VarName start) {
        return new ValueFlagFragment(start);
    }

    public static NotCastingFragment notCasting(VarName start) {
        return new NotCastingFragment(start);
    }

    public static Fragment neq(VarName start, VarName other) {
        return new NeqFragment(start, other);
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> outSubs(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.out(SUB.getLabel())).emit()).unfold();
    }

    @SuppressWarnings("unchecked")
    static GraphTraversal<Vertex, Vertex> inSubs(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.in(SUB.getLabel())).emit()).unfold();
    }
}
