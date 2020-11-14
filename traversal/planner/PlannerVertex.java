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
 */

package grakn.core.traversal.planner;

import grakn.core.traversal.Identifier;
import grakn.core.traversal.structure.StructureVertex;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

class PlannerVertex {

    private final Planner planner;
    private final Identifier identifier;
    private final Set<PlannerEdge> outgoing;
    private final Set<PlannerEdge> incoming;
    private Set<StructureVertex.Property> properties;

    PlannerVertex(Planner planner, Identifier identifier) {
        this.planner = planner;
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

    void out(PlannerEdge edge) {
        outgoing.add(edge);
    }

    void in(PlannerEdge edge) {
        incoming.add(edge);
    }

    void properties(Set<StructureVertex.Property> properties) {
        this.properties = properties;
    }

    Identifier identifier() {
        return identifier;
    }

    Set<PlannerEdge> outs() {
        return outgoing;
    }

    Set<PlannerEdge> ins() {
        return incoming;
    }

    void initalise() {
        // TODO
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlannerVertex that = (PlannerVertex) o;
        return (this.identifier.equals(that.identifier) && this.properties.equals(that.properties));
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, properties);
    }
}
