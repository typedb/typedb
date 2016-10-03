/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.pattern.property;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RoleType;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.Fragment;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;

import java.util.Collection;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_BOUNDED;
import static io.mindmaps.graql.internal.gremlin.Traversals.inAkos;
import static io.mindmaps.graql.internal.gremlin.Traversals.outAkos;
import static io.mindmaps.util.Schema.EdgeLabel.PLAYS_ROLE;

public class PlaysRoleProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin role;

    public PlaysRoleProperty(VarAdmin role) {
        this.role = role;
    }

    public VarAdmin getRole() {
        return role;
    }

    @Override
    public String getName() {
        return "plays-role";
    }

    @Override
    public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> matchProperty(String start) {
        return Sets.newHashSet(MultiTraversal.create(
                Fragment.create(
                        t -> inAkos(outAkos(t).out(PLAYS_ROLE.getLabel())),
                        EDGE_BOUNDED, start, role.getName()
                ),
                Fragment.create(
                        t -> inAkos(outAkos(t).in(PLAYS_ROLE.getLabel())),
                        EDGE_BOUNDED, role.getName(), start
                )
        ));
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(role);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(role);
    }

    @Override
    public void insertProperty(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        RoleType roleType = insertQueryExecutor.getConcept(role).asRoleType();
        concept.asType().playsRole(roleType);
    }

    @Override
    public void deleteProperty(MindmapsGraph graph, Concept concept) {
        String roleId = role.getId().orElseThrow(() -> failDelete(this));
        concept.asType().deletePlaysRole(graph.getRoleType(roleId));
    }
}
