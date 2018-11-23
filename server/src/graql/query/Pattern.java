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

package grakn.core.graql.query;

import grakn.core.graql.admin.PatternAdmin;

import javax.annotation.CheckReturnValue;

/**
 * A pattern describing a subgraph.
 * <p>
 * A {@code Pattern} can describe an entire graph, or just a single concept.
 * <p>
 * For example, {@code var("x").isa("movie")} is a pattern representing things that are movies.
 * <p>
 * A pattern can also be a conjunction: {@code and(var("x").isa("movie"), var("x").value("Titanic"))}, or a disjunction:
 * {@code or(var("x").isa("movie"), var("x").isa("tv-show"))}. These can be used to combine other patterns together
 * into larger patterns.
 *
 */
public interface Pattern {

    /**
     * @return an Admin class that allows inspecting or manipulating this pattern
     */
    @CheckReturnValue
    PatternAdmin admin();

    /**
     * Join patterns in a conjunction
     */
    @CheckReturnValue
    Pattern and(Pattern pattern);

    /**
     * Join patterns in a disjunction
     */
    @CheckReturnValue
    Pattern or(Pattern pattern);
}
