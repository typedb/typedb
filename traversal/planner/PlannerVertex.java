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

import java.util.HashSet;
import java.util.Set;

class PlannerVertex {

    private final Identifier identifier;
    private final Planner planner;
    private final Set<PlannerEdge> outgoing;
    private final Set<PlannerEdge> incoming;

    PlannerVertex(Identifier identifier, Planner planner) {
        this.identifier = identifier;
        this.planner = planner;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

    void out(PlannerEdge edge) {
        // TODO
    }

    void in(PlannerEdge edge) {
        // TODO
    }

    Set<PlannerEdge> outs() {
        return outgoing;
    }

    Set<PlannerEdge> ins() {
        return incoming;
    }

    Identifier identifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlannerVertex that = (PlannerVertex) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
}
