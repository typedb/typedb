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

package ai.grakn.client.concept;

import ai.grakn.client.Grakn;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.rpc.proto.ConceptProto;
import com.google.auto.value.AutoValue;

import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Attribute}
 *
 * @param <D> The data type of this attribute
 */
@AutoValue
public abstract class RemoteAttribute<D> extends RemoteThing<Attribute<D>, AttributeType<D>> implements Attribute<D> {

    public static <D> RemoteAttribute<D> create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteAttribute<>(tx, id);
    }

    @Override
    public final D value() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeValue(ConceptProto.Attribute.Value.Req.getDefaultInstance()).build();

        ConceptProto.ValueObject value = runMethod(method).getAttributeValue().getValue();
        // TODO: Fix this unsafe casting
        return (D) value.getAllFields().values().iterator().next();
    }

    @Override
    public final Stream<Thing> owners() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeOwners(ConceptProto.Attribute.Owners.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getAttributeOwners().getId();
        return conceptStream(iteratorId, res -> res.getAttributeOwners().getConcept()).map(Concept::asThing);
    }

    @Override
    public final AttributeType.DataType<D> dataType() {
        return type().dataType();
    }

    @Override
    final AttributeType<D> asCurrentType(Concept concept) {
        return concept.asAttributeType();
    }

    @Override
    final Attribute<D> asCurrentBaseType(Concept other) {
        return other.asAttribute();
    }
}
