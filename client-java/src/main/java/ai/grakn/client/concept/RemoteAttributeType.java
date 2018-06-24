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

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.client.Grakn;
import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

/**
 * @author Felix Chapman
 *
 * @param <D> The data type of this attribute type
 */
@AutoValue
public abstract class RemoteAttributeType<D> extends RemoteType<AttributeType<D>, Attribute<D>> implements AttributeType<D> {

    public static <D> RemoteAttributeType<D> create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteAttributeType<>(tx, id);
    }

    @Override
    public final AttributeType<D> setRegex(String regex) {
        runMethod(GrpcConcept.ConceptMethod.newBuilder().setSetRegex(regex).build());
        return asCurrentBaseType(this);
    }

    @Override
    public final Attribute<D> putAttribute(D value) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setPutAttribute(ConceptBuilder.attributeValue(value));
        GrpcGrakn.TxResponse response = runMethod(method.build());
        Concept concept = ConceptBuilder.concept(response.getConceptResponse().getConcept(), tx());

        return asInstance(concept);
    }

    @Nullable
    @Override
    public final Attribute<D> getAttribute(D value) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetAttribute(ConceptBuilder.attributeValue(value));
        GrpcGrakn.TxResponse response = runMethod(method.build());

        if (response.getConceptResponse().getNoResult()) return null;

        Concept concept = ConceptBuilder.concept(response.getConceptResponse().getConcept(), tx());
        return concept.asAttribute();
    }

    @Nullable
    @Override
    public final AttributeType.DataType<D> getDataType() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetDataTypeOfAttributeType(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());

        if (response.getConceptResponse().getNoResult()) return null;
        return (AttributeType.DataType<D>) ConceptBuilder.dataType(response.getConceptResponse().getDataType());
    }

    @Nullable
    @Override
    public final String getRegex() {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetRegex(GrpcConcept.Unit.getDefaultInstance());
        GrpcGrakn.TxResponse response = runMethod(method.build());

        if (response.getConceptResponse().getNoResult()) return null;
        return response.getConceptResponse().getRegex();
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
