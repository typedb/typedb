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

package hypergraph.concept.thing.impl;

import hypergraph.concept.thing.Attribute;
import hypergraph.concept.thing.Relation;
import hypergraph.concept.thing.Thing;
import hypergraph.concept.type.AttributeType;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.impl.ThingTypeImpl;
import hypergraph.graph.vertex.ThingVertex;

import java.util.Objects;
import java.util.stream.Stream;

public abstract class ThingImpl implements Thing {

    protected final ThingVertex vertex;

    protected ThingImpl(ThingVertex vertex) {
        this.vertex = Objects.requireNonNull(vertex);
    }

    @Override
    public abstract ThingTypeImpl type();

    @Override
    public boolean isInferred() {
        return vertex.isInferred();
    }

    @Override
    public Thing has(Attribute attribute) {
        return null;
    }

    @Override
    public void unhas(Attribute attribute) {

    }

    @Override
    public Stream<? extends Attribute> keys(AttributeType... attributeTypes) {
        return null;
    }

    @Override
    public Stream<? extends Attribute> attributes(AttributeType... attributeTypes) {
        return null;
    }

    @Override
    public Stream<? extends RoleType> roles() {
        return null;
    }

    @Override
    public Stream<? extends Relation> relations(RoleType... roleTypes) {
        return null;
    }

    @Override
    public byte[] iid() {
        return new byte[0];
    }
}
