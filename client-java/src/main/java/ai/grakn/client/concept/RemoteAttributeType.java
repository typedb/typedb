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
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.util.CommonUtil;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * Client implementation of {@link ai.grakn.concept.AttributeType}
 *
 * @param <D> The data type of this attribute type
 */
@AutoValue
public abstract class RemoteAttributeType<D> extends RemoteType<AttributeType<D>, Attribute<D>> implements AttributeType<D> {

    public static <D> RemoteAttributeType<D> create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteAttributeType<>(tx, id);
    }

    @Override
    public final Attribute<D> create(D value) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeCreate(ConceptProto.AttributeType.Create.Req.newBuilder()
                        .setValue(ConceptBuilder.attributeValue(value))).build();

        Concept concept = ConceptBuilder.concept(runMethod(method).getAttributeTypeCreate().getConcept(), tx());
        return asInstance(concept);
    }

    @Nullable
    @Override
    public final Attribute<D> attribute(D value) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeAttribute(ConceptProto.AttributeType.Attribute.Req.newBuilder()
                        .setValue(ConceptBuilder.attributeValue(value))).build();

        ConceptProto.AttributeType.Attribute.Res response = runMethod(method).getAttributeTypeAttribute();
        switch (response.getResCase()) {
            case NULL:
                return null;
            case CONCEPT:
                return ConceptBuilder.concept(response.getConcept(), tx()).asAttribute();
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final AttributeType.DataType<D> dataType() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeDataType(ConceptProto.AttributeType.DataType.Req.getDefaultInstance()).build();

        ConceptProto.AttributeType.DataType.Res response = runMethod(method).getAttributeTypeDataType();
        switch (response.getResCase()) {
            case NULL:
                return null;
            case DATATYPE:
                return (AttributeType.DataType<D>) ConceptBuilder.dataType(response.getDataType());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final String regex() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeGetRegex(ConceptProto.AttributeType.GetRegex.Req.getDefaultInstance()).build();

        String regex = runMethod(method).getAttributeTypeGetRegex().getRegex();
        return regex.isEmpty() ? null : regex;
    }

    @Override
    public final AttributeType<D> regex(String regex) {
        if (regex == null) regex = "";
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setAttributeTypeSetRegex(ConceptProto.AttributeType.SetRegex.Req.newBuilder()
                        .setRegex(regex)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    final AttributeType<D> asCurrentBaseType(Concept other) {
        return other.asAttributeType();
    }

    @Override
    final boolean equalsCurrentBaseType(Concept other) {
        return other.isAttributeType();
    }

    @Override
    protected final Attribute<D> asInstance(Concept concept) {
        return concept.asAttribute();
    }
}
