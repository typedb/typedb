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

package grakn.core.graql.planning.gremlin.sets;

import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;

import javax.annotation.CheckReturnValue;
import java.util.Iterator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public abstract class EquivalentFragmentSetImpl implements EquivalentFragmentSet, Iterable<Fragment> {

    private final VarProperty varProperty;

    public EquivalentFragmentSetImpl(VarProperty varProperty) {
        this.varProperty = varProperty;
    }

    public VarProperty varProperty() {
        return varProperty;
    }

    @Override
    @CheckReturnValue
    public Iterator<Fragment> iterator() {
        return stream().iterator();
    }

    @Override
    public Stream<Fragment> stream() {
        return fragments().stream();
    }

    @Override
    public String toString() {
        return fragments().stream().map(Object::toString).collect(joining(", ", "{", "}"));
    }
}
