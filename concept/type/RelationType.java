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

package grakn.concept.type;

import grakn.concept.thing.Relation;

import java.util.stream.Stream;

public interface RelationType extends ThingType {

    @Override
    default RelationType asRelationType() { return this; }

    @Override
    RelationType sup();

    @Override
    Stream<? extends RelationType> sups();

    @Override
    Stream<? extends RelationType> subs();

    @Override
    Stream<? extends Relation> instances();

    void sup(RelationType superType);

    void relates(String roleLabel);

    void relates(String roleLabel, String overriddenLabel);

    void unrelate(String roleLabel);

    Stream<? extends RoleType> roles();

    RoleType role(String roleLabel);

    Relation create();

    Relation create(boolean isInferred);
}
