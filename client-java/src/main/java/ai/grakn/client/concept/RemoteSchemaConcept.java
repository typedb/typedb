/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
