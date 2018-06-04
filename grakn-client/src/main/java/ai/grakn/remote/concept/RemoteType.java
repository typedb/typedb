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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote.concept;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.ConceptMethod;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 * @param <Instance> the exact type of instances of this class
 */
abstract class RemoteType<Self extends Type, Instance extends Thing> extends RemoteSchemaConcept<Self> implements Type {

    @Override
    public final Self setAbstract(Boolean isAbstract) throws GraknTxOperationException {
        return runVoidMethod(ConceptMethod.setAbstract(isAbstract));
    }

    @Override
    public final Self plays(Role role) throws GraknTxOperationException {
        return runVoidMethod(ConceptMethod.setRolePlayedByType(role));
    }

    @Override
    public final Self key(AttributeType attributeType) throws GraknTxOperationException {
        return runVoidMethod(ConceptMethod.setKeyType(attributeType));
    }

    @Override
    public final Self attribute(AttributeType attributeType) throws GraknTxOperationException {
        return runVoidMethod(ConceptMethod.setAttributeType(attributeType));
    }

    @Override
    public final Stream<Role> plays() {
        return runMethod(ConceptMethod.GET_ROLES_PLAYED_BY_TYPE).map(Concept::asRole);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        return runMethod(ConceptMethod.GET_ATTRIBUTE_TYPES).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<AttributeType> keys() {
        return runMethod(ConceptMethod.GET_KEY_TYPES).map(Concept::asAttributeType);
    }

    @Override
    public final Stream<Instance> instances() {
        return runMethod(ConceptMethod.GET_INSTANCES).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        return runMethod(ConceptMethod.IS_ABSTRACT);
    }

    @Override
    public final Self deletePlays(Role role) {
        return runVoidMethod(ConceptMethod.unsetRolePlayedByType(role));
    }

    @Override
    public final Self deleteAttribute(AttributeType attributeType) {
        return runVoidMethod(ConceptMethod.unsetAttributeType(attributeType));
    }

    @Override
    public final Self deleteKey(AttributeType attributeType) {
        return runVoidMethod(ConceptMethod.unsetKeyType(attributeType));
    }

    protected abstract Instance asInstance(Concept concept);
}
