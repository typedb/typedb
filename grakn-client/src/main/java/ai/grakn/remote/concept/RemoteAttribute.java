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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.rpc.ConceptMethods;
import ai.grakn.remote.RemoteGraknTx;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * @author Felix Chapman
 *
 * @param <D> The data type of this attribute
 */
@AutoValue
abstract class RemoteAttribute<D> extends RemoteThing<Attribute<D>, AttributeType<D>> implements Attribute<D> {

    public static <D> RemoteAttribute<D> create(RemoteGraknTx tx, ConceptId id) {
        return new AutoValue_RemoteAttribute<>(tx, id);
    }

    @Override
    public final D getValue() {
        return (D) runMethod(ConceptMethods.GET_VALUE);
    }

    @Override
    public final AttributeType.DataType<D> dataType() {
        return (AttributeType.DataType<D>) runMethod(ConceptMethods.GET_DATA_TYPE_OF_ATTRIBUTE);
    }

    @Override
    public final Stream<Thing> ownerInstances() {
        return runMethod(ConceptMethods.GET_OWNERS).map(Concept::asThing);
    }

    @Override
    final AttributeType<D> asMyType(Concept concept) {
        return concept.asAttributeType();
    }

    @Override
    final Attribute<D> asSelf(Concept concept) {
        return concept.asAttribute();
    }
}
