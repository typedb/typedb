/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.gremlin;

import ai.grakn.graql.Streamable;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * a pattern to match in the graph. comprised of {@code Fragments}, each describing one way to represent the traversal,
 * starting from different variables.
 * <p>
 * A {@code EquivalentFragmentSet} may contain only one {@code Fragment} (e.g. checking the 'id' property), while others may
 * be comprised of two fragments (e.g. $x isa $y, which may start from $x or $y).
 *
 * @author Felix Chapman
 */
public abstract class EquivalentFragmentSet implements Streamable<Fragment> {

    /**
     * @return a set of fragments that this EquivalentFragmentSet contains
     */
    public abstract Set<Fragment> fragments();

    @Nullable
    public abstract VarProperty varProperty();

    @Override
    public String toString() {
        return fragments().stream().map(Object::toString).collect(joining(", ", "{", "}"));
    }

    @Override
    public final Stream<Fragment> stream() {
        return fragments().stream();
    }
}
