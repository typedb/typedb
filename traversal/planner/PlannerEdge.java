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

import java.util.Arrays;
import java.util.Objects;

class PlannerEdge {

    private final PlannerVertex from;
    private final PlannerVertex to;
    private final boolean isTransitive;
    private final String[] labels;
    private final int hash;

    PlannerEdge(PlannerVertex from, PlannerVertex to,
                boolean isTranstive, String[] labels) {
        this.from = from;
        this.to = to;
        this.isTransitive = isTranstive;
        this.labels = labels;
        this.hash = Objects.hash(from, to, isTranstive, Arrays.hashCode(labels));
    }

    PlannerVertex from() {
        return from;
    }

    PlannerVertex to() {
        return to;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final PlannerEdge that = (PlannerEdge) object;
        return (this.from.equals(that.from) &&
                this.to.equals(that.to) &&
                this.isTransitive == that.isTransitive &&
                Arrays.equals(this.labels, that.labels));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
