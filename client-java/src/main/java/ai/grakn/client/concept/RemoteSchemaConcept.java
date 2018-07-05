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

import ai.grakn.client.rpc.ConceptBuilder;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.rpc.proto.IteratorProto;
import ai.grakn.rpc.proto.MethodProto;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.util.CommonUtil;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.SchemaConcept}
 *
 * @param <SomeType> The exact type of this class
 */
abstract class RemoteSchemaConcept<SomeType extends SchemaConcept> extends RemoteConcept<SomeType> implements SchemaConcept {

    public final SomeType sup(SomeType type) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetDirectSuperConcept(MethodProto.SetDirectSuperConcept.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(type))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    public final SomeType sub(SomeType type) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetDirectSuperConcept(MethodProto.SetDirectSuperConcept.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(this))).build();

        runMethod(type.id(), method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Label label() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetLabel(MethodProto.GetLabel.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        return Label.of(response.getConceptMethod().getResponse().getGetLabel().getLabel());
    }

    @Override
    public final Boolean isImplicit() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setIsImplicit(MethodProto.IsImplicit.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        return response.getConceptMethod().getResponse().getIsImplicit().getImplicit();
    }

    @Override
    public final SomeType label(Label label) {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setSetLabel(MethodProto.SetLabel.Req.newBuilder()
                        .setLabel(label.getValue())).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Nullable
    @Override
    public final SomeType sup() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetDirectSuperConcept(MethodProto.GetDirectSuperConcept.Req.getDefaultInstance()).build();

        SessionProto.Transaction.Res response = runMethod(method);
        MethodProto.GetDirectSuperConcept.Res responseConcept = response.getConceptMethod().getResponse().getGetDirectSuperConcept();

        switch (responseConcept.getResCase()) {
            case NULL:
                return null;
            case CONCEPT:
                Concept concept = ConceptBuilder.concept(responseConcept.getConcept(), tx());
                return equalsCurrentBaseType(concept) ? asCurrentBaseType(concept) : null;
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }

    }

    @Override
    public final Stream<SomeType> sups() {
        return tx().admin().sups(this).filter(this::equalsCurrentBaseType).map(this::asCurrentBaseType);
    }

    @Override
    public final Stream<SomeType> subs() {
        MethodProto.Method.Req method = MethodProto.Method.Req.newBuilder()
                .setGetSubConcepts(MethodProto.GetSubConcepts.Req.getDefaultInstance()).build();

        IteratorProto.IteratorId iteratorId = runMethod(method).getConceptMethod().getResponse().getGetSubConcepts().getIteratorId();
        return conceptStream(iteratorId).map(this::asCurrentBaseType);
    }

    @Override
    public final LabelId labelId() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public final Stream<Rule> whenRules() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public final Stream<Rule> thenRules() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    abstract boolean equalsCurrentBaseType(Concept other);
}
