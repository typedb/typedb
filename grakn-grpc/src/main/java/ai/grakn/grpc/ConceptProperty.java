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

package ai.grakn.grpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.ConceptPropertyValue;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.IsImplicit;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.IsInferred;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.LabelProperty;

/**
 * Wrapper for describing {@link Concept} properties that can be communicated over gRPC.
 *
 *
 * @author Felix Chapman
 *
 * @param <T> The type of the concept property value.
 */
public abstract class ConceptProperty<T> {

    public static final ConceptProperty<Object> VALUE = create(null, null, null, null);

    public static final ConceptProperty<AttributeType.DataType<?>> DATA_TYPE = create(null, null, null, null);

    public static final ConceptProperty<Label> LABEL = create(LabelProperty,
            concept -> concept.asSchemaConcept().getLabel(),
            val -> GrpcUtil.convert(val.getLabel()),
            (builder, val) -> builder.setLabel(GrpcUtil.convert(val)));

    public static final ConceptProperty<Boolean> IS_IMPLICIT = create(IsImplicit,
            concept -> concept.asSchemaConcept().isImplicit(),
            ConceptPropertyValue::getIsImplicit, ConceptPropertyValue.Builder::setIsImplicit);

    public static final ConceptProperty<Boolean> IS_INFERRED = create(IsInferred,
            concept -> concept.asThing().isInferred(),
            ConceptPropertyValue::getIsInferred, ConceptPropertyValue.Builder::setIsInferred);

    public static final ConceptProperty<Boolean> IS_ABSTRACT = create(null, null, null, null);

    public static final ConceptProperty<Pattern> WHEN = create(null, null, null, null);

    public static final ConceptProperty<Pattern> THEN = create(null, null, null, null);

    public static final ConceptProperty<String> REGEX = create(null, null, null, null);

    @Nullable
    public static ConceptProperty<?> fromGrpc(GraknOuterClass.ConceptProperty conceptProperty) {
        switch (conceptProperty) {
            case ValueProperty:
                return VALUE;
            case DataTypeProperty:
                return DATA_TYPE;
            case LabelProperty:
                return LABEL;
            case IsImplicit:
                return IS_IMPLICIT;
            case IsInferred:
                return IS_INFERRED;
            case IsAbstract:
                return IS_ABSTRACT;
            case When:
                return WHEN;
            case Then:
                return THEN;
            case Regex:
                return REGEX;
            default:
            case UNRECOGNIZED:
                return null;
        }
    }

    public final TxResponse response(T value) {
        ConceptPropertyValue.Builder conceptPropertyValue = ConceptPropertyValue.newBuilder();
        set(conceptPropertyValue, value);
        return TxResponse.newBuilder().setConceptPropertyValue(conceptPropertyValue.build()).build();
    }

    public abstract TxResponse response(Concept concept);

    public abstract T get(TxResponse value);

    abstract void set(ConceptPropertyValue.Builder builder, T value);

    abstract GraknOuterClass.ConceptProperty toGrpc();

    private static <T> ConceptProperty<T> create(
            GraknOuterClass.ConceptProperty grpcProperty,
            Function<Concept, T> conceptGetter,
            Function<ConceptPropertyValue, T> responseGetter,
            BiConsumer<ConceptPropertyValue.Builder, T> setter
    ) {
        return new ConceptProperty<T>() {
            @Override
            public TxResponse response(Concept concept) {
                return response(conceptGetter.apply(concept));
            }

            @Override
            public T get(TxResponse txResponse) {
                return responseGetter.apply(txResponse.getConceptPropertyValue());
            }

            @Override
            void set(ConceptPropertyValue.Builder builder, T value) {
                setter.accept(builder, value);
            }

            @Override
            GraknOuterClass.ConceptProperty toGrpc() {
                return grpcProperty;
            }
        };
    }
}
