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

package ai.grakn.remote.concept;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.grpc.ConceptMethod;

import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;

/**
 * @author Felix Chapman
 *
 * @param <Self> The exact type of this class
 * @param <Instance> the exact type of instances of this class
 */
abstract class RemoteType<Self extends Type, Instance extends Thing> extends RemoteSchemaConcept<Self> implements Type {

    @Override
    public final Self setAbstract(Boolean isAbstract) throws GraknTxOperationException {
        if (isAbstract) {
            return define(ME.isAbstract());
        } else {
            return undefine(ME.isAbstract());
        }
    }

    @Override
    public final Self plays(Role role) throws GraknTxOperationException {
        return define(role, ME.plays(TARGET));
    }

    @Override
    public final Self key(AttributeType attributeType) throws GraknTxOperationException {
        return define(ME.key(var().label(attributeType.getLabel())));
    }

    @Override
    public final Self attribute(AttributeType attributeType) throws GraknTxOperationException {
        return define(ME.has(var().label(attributeType.getLabel())));
    }

    @Override
    public final Stream<Role> plays() {
        return query(ME.plays(TARGET)).map(Concept::asRole);
    }

    @Override
    public final Stream<AttributeType> attributes() {
        return runMethod(ConceptMethod.GET_ATTRIBUTE_TYPES);
    }

    @Override
    public final Stream<AttributeType> keys() {
        return runMethod(ConceptMethod.GET_KEY_TYPES);
    }

    @Override
    public final Stream<Instance> instances() {
        return query(TARGET.isa(ME)).map(this::asInstance);
    }

    @Override
    public final Boolean isAbstract() {
        return runMethod(ConceptMethod.IS_ABSTRACT);
    }

    @Override
    public final Self deletePlays(Role role) {
        return undefine(role, ME.plays(TARGET));
    }

    @Override
    public final Self deleteAttribute(AttributeType attributeType) {
        return undefine(ME.has(var().label(attributeType.getLabel())));
    }

    @Override
    public final Self deleteKey(AttributeType attributeType) {
        return undefine(ME.key(var().label(attributeType.getLabel())));
    }

    protected abstract Instance asInstance(Concept concept);
}
