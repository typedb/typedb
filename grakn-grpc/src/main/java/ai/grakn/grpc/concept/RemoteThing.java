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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc.concept;

import ai.grakn.GraknTx;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteThing extends RemoteConcept implements Thing {

    public static RemoteThing create(GraknTx tx, ConceptId id) {
        return new AutoValue_RemoteThing(tx, id);
    }

    @Override
    public final Type type() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Relationship> relationships(Role... roles) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Role> plays() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Thing attribute(Attribute attribute) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Relationship attributeRelationship(Attribute attribute) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Attribute<?>> attributes(AttributeType... attributeTypes) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Attribute<?>> keys(AttributeType... attributeTypes) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Thing deleteAttribute(Attribute attribute) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final boolean isInferred() {
        throw new UnsupportedOperationException(); // TODO: implement
    }
}
