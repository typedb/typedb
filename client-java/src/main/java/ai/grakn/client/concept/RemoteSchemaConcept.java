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
import ai.grakn.rpc.proto.ConceptProto;
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
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSetSup(ConceptProto.SchemaConcept.SetSup.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(type))).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    public final SomeType sub(SomeType type) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSetSup(ConceptProto.SchemaConcept.SetSup.Req.newBuilder()
                        .setConcept(ConceptBuilder.concept(this))).build();

        runMethod(type.id(), method);
        return asCurrentBaseType(this);
    }

    @Override
    public final Label label() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptGetLabel(ConceptProto.SchemaConcept.GetLabel.Req.getDefaultInstance()).build();

        return Label.of(runMethod(method).getSchemaConceptGetLabel().getLabel());
    }

    @Override
    public final Boolean isImplicit() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptIsImplicit(ConceptProto.SchemaConcept.IsImplicit.Req.getDefaultInstance()).build();

        return runMethod(method).getSchemaConceptIsImplicit().getImplicit();
    }

    @Override
    public final SomeType label(Label label) {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSetLabel(ConceptProto.SchemaConcept.SetLabel.Req.newBuilder()
                        .setLabel(label.getValue())).build();

        runMethod(method);
        return asCurrentBaseType(this);
    }

    @Nullable
    @Override
    public final SomeType sup() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptGetSup(ConceptProto.SchemaConcept.GetSup.Req.getDefaultInstance()).build();

        ConceptProto.SchemaConcept.GetSup.Res response = runMethod(method).getSchemaConceptGetSup();

        switch (response.getResCase()) {
            case NULL:
                return null;
            case CONCEPT:
                Concept concept = ConceptBuilder.concept(response.getConcept(), tx());
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
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setSchemaConceptSubs(ConceptProto.SchemaConcept.Subs.Req.getDefaultInstance()).build();

        int iteratorId = runMethod(method).getSchemaConceptSubs().getId();
        return conceptStream(iteratorId, res -> res.getSchemaConceptSubs().getConcept()).map(this::asCurrentBaseType);
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
