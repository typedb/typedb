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

import grakn.core.traversal.property.EdgeProperty;

import java.util.Objects;

class PlannerEdge {

    private final EdgeProperty property;
    private final PlannerVertex from;
    private final PlannerVertex to;
    private final int hash;

    PlannerEdge(EdgeProperty property, PlannerVertex from, PlannerVertex to) {
        this.property = property;
        this.from = from;
        this.to = to;
        this.hash = Objects.hash(this.property, this.from, this.to);
    }

    PlannerVertex from() {
        return from;
    }

    PlannerVertex to() {
        return to;
    }

    void initialise() {
        // TODO
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final PlannerEdge that = (PlannerEdge) object;
        return (this.property.equals(that.property) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
