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

package grakn.core.kb.graql.planning;

import grakn.core.common.exception.GraknException;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;

public class GraknQueryPlannerException extends GraknException {

    GraknQueryPlannerException(String error) {
        super(error);
    }

    GraknQueryPlannerException(String error, Exception e) {
        super(error, e);
    }

    public static GraknQueryPlannerException unrootedPlanningNode(Node node) {
        return new GraknQueryPlannerException("QueryPlanner node: " + node.toString() + " has no parent");
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static GraknQueryPlannerException create(String error) {
        return new GraknQueryPlannerException(error);
    }
}
