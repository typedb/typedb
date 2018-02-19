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
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteType extends RemoteSchemaConcept<Type> implements Type {

    public static RemoteType create(GraknTx tx, ConceptId id, Label label, boolean isImplicit) {
        return new AutoValue_RemoteType(tx, id, label, isImplicit);
    }

    @Override
    public final Type setAbstract(Boolean isAbstract) throws GraknTxOperationException {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Type plays(Role role) throws GraknTxOperationException {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Type key(AttributeType attributeType) throws GraknTxOperationException {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Type attribute(AttributeType attributeType) throws GraknTxOperationException {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<Role> plays() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<AttributeType> attributes() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<AttributeType> keys() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Stream<? extends Thing> instances() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Boolean isAbstract() {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Type deletePlays(Role role) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Type deleteAttribute(AttributeType attributeType) {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final Type deleteKey(AttributeType attributeType) {
        throw new UnsupportedOperationException(); // TODO: implement
    }
}
