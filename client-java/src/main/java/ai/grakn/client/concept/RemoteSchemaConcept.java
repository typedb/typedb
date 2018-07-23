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

import ai.grakn.client.rpc.RequestBuilder;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.util.CommonUtil;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.SchemaConcept}
 *
 * @param <SomeSchemaConcept> The exact type of this class
 */
abstract class RemoteSchemaConcept<SomeSchemaConcept extends SchemaConcept> extends RemoteConcept<SomeSchemaConcept> implements SchemaConcept {

    public final SomeSchemaConcept sup(SomeSchemaConcept type) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSetSupReq(ConceptProto.SchemaConcept.SetSup.Req.newBuilder()
                        .setSchemaConcept(RequestBuilder.Concept.concept(type))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    public final SomeSchemaConcept sub(SomeSchemaConcept type) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSetSupReq(ConceptProto.SchemaConcept.SetSup.Req.newBuilder()
                        .setSchemaConcept(RequestBuilder.Concept.concept(this))).build();

        runMethod(type.id(), method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Label label() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptGetLabelReq(ConceptProto.SchemaConcept.GetLabel.Req.getDefaultInstance()).build();

        return Label.of(runMethod(method).getSchemaConceptGetLabelRes().getLabel());
    }

    @Override
    public final Boolean isImplicit() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptIsImplicitReq(ConceptProto.SchemaConcept.IsImplicit.Req.getDefaultInstance()).build();

        return runMethod(method).getSchemaConceptIsImplicitRes().getImplicit();
    }

    @Override
    public final SomeSchemaConcept label(Label label) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSetLabelReq(ConceptProto.SchemaConcept.SetLabel.Req.newBuilder()
                        .setLabel(label.getValue())).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Nullable
    @Override
    public final SomeSchemaConcept sup() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptGetSupReq(ConceptProto.SchemaConcept.GetSup.Req.getDefaultInstance()).build();

        ConceptProto.SchemaConcept.GetSup.Res response = runMethod(method).getSchemaConceptGetSupRes();

        switch (response.getResCase()) {
            case NULL:
                return null;
            case SCHEMACONCEPT:
                Concept concept = RemoteConcept.of(response.getSchemaConcept(), tx());
                return equalsCurrentBaseType(concept) ? asCurrentBaseType(concept) : null;
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }

    }

    @Override
    public final Stream<SomeSchemaConcept> sups() {
        return tx().admin().sups(this).filter(this::equalsCurrentBaseType).map(this::asCurrentBaseType);
    }

    @Override
    public final Stream<SomeSchemaConcept> subs() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSubsReq(ConceptProto.SchemaConcept.Subs.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getSchemaConceptSubsIter().getId();
        return conceptStream(iteratorId, res -> res.getSchemaConceptSubsIterRes().getSchemaConcept()).map(this::asCurrentBaseType);
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
