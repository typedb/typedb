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
import ai.grakn.rpc.proto.MethodProto;
import ai.grakn.rpc.proto.SessionProto;
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

    @Nullable
    @Override
    public final String regex() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetRegex(MethodProto.GetRegex.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        String regex = response.getConceptMethod().getResponse().getGetRegex().getRegex();
        return regex.isEmpty() ? null : regex;
    }

    @Override
    public final AttributeType<D> regex(String regex) {
        if (regex == null) regex = "";
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetRegex(MethodProto.SetRegex.Req.newBuilder()
                        .setRegex(regex)).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Attribute<D> create(D value) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setPutAttribute(MethodProto.PutAttribute.Req.newBuilder()
                        .setValue(ConceptBuilder.attributeValue(value))).build();

        SessionProto.Transaction.Res response = runMethod(method);
        Concept concept = ConceptBuilder.concept(response.getConceptMethod().getResponse().getPutAttribute().getConcept(), tx());
        return asInstance(concept);
    }

    @Nullable
    @Override
    public final Attribute<D> attribute(D value) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetAttribute(MethodProto.GetAttribute.Req.newBuilder()
                        .setValue(ConceptBuilder.attributeValue(value))).build();

        SessionProto.Transaction.Res response = runMethod(method);
        MethodProto.GetAttribute.Res methodResponse = response.getConceptMethod().getResponse().getGetAttribute();
        switch (methodResponse.getResCase()) {
            case NULL:
                return null;
            case CONCEPT:
                return ConceptBuilder.concept(methodResponse.getConcept(), tx()).asAttribute();
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final AttributeType.DataType<D> dataType() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetDataTypeOfAttributeType(MethodProto.GetDataTypeOfAttributeType.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        MethodProto.GetDataTypeOfAttributeType.Res methodResponse = response.getConceptMethod().getResponse().getGetDataTypeOfAttributeType();
        switch (methodResponse.getResCase()) {
            case NULL:
                return null;
            case DATATYPE:
                return (AttributeType.DataType<D>) ConceptBuilder.dataType(methodResponse.getDataType());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
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
