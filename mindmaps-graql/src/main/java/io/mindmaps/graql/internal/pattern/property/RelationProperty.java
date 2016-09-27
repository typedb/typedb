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

import io.mindmaps.graql.admin.VarAdmin;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class RelationProperty implements VarProperty {

    private final Set<VarAdmin.Casting> castings = new HashSet<>();

    public void addCasting(VarAdmin.Casting casting) {
        castings.add(casting);
    }

    public Stream<VarAdmin.Casting> getCastings() {
        return castings.stream();
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("(").append(castings.stream().map(Object::toString).collect(joining(", "))).append(")");
    }
}
