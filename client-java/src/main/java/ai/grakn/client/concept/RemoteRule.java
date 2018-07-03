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
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.proto.ConceptProto;
import ai.grakn.rpc.proto.SessionProto;
import ai.grakn.util.CommonUtil;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class RemoteRule extends RemoteSchemaConcept<Rule> implements Rule {

    public static RemoteRule create(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRule(tx, id);
    }

    @Nullable
    @Override
    public final Pattern getWhen() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetWhen(ConceptProto.GetWhen.Req.getDefaultInstance()).build();
        SessionProto.Transaction.Res response = runMethod(method);

        ConceptProto.GetWhen.Res whenResponse = response.getConceptMethod().getResponse().getGetWhen();
        switch (whenResponse.getResCase()) {
            case NULL:
                return null;
            case PATTERN:
                return Graql.parser().parsePattern(whenResponse.getPattern());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final Pattern getThen() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setGetThen(ConceptProto.GetThen.Req.getDefaultInstance()).build();
        SessionProto.Transaction.Res response = runMethod(method);

        ConceptProto.GetThen.Res thenResponse = response.getConceptMethod().getResponse().getGetThen();
        switch (thenResponse.getResCase()) {
            case NULL:
                return null;
            case PATTERN:
                return Graql.parser().parsePattern(thenResponse.getPattern());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Override
    public final Stream<Type> getHypothesisTypes() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public final Stream<Type> getConclusionTypes() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    final Rule asCurrentBaseType(Concept other) {
        return other.asRule();
    }

    @Override
    final boolean equalsCurrentBaseType(Concept other) {
        return other.isRule();
    }
}
