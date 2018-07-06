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
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Thing;
import ai.grakn.rpc.proto.AnswerProto;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.SessionProto;
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
                .setGetValue(ConceptProto.GetValue.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        ConceptProto.AttributeValue attributeValue = response.getConceptMethod().getResponse().getGetValue().getValue();
        // TODO: Fix this unsafe casting
        return (D) attributeValue.getAllFields().values().iterator().next();
    }

    @Override
    public final Stream<Thing> owners() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetOwners(ConceptProto.GetOwners.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetOwners().getIteratorId();
        return conceptStream(iteratorId).map(Concept::asThing);
    }

    @Override
    public final AttributeType.DataType<D> dataType() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetDataTypeOfAttribute(ConceptProto.GetDataTypeOfAttribute.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        // TODO: Fix this unsafe casting
        return (AttributeType.DataType<D>) ConceptBuilder.dataType(response.getConceptMethod().getResponse()
                .getGetDataTypeOfAttribute().getDataType());
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
