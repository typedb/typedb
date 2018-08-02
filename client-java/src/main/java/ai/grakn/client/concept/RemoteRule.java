/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
import ai.grakn.util.CommonUtil;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * Client implementation of {@link ai.grakn.concept.Rule}
 */
@AutoValue
public abstract class RemoteRule extends RemoteSchemaConcept<Rule> implements Rule {

    static RemoteRule construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteRule(tx, id);
    }

    @Nullable
    @Override
    public final Pattern when() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRuleWhenReq(ConceptProto.Rule.When.Req.getDefaultInstance()).build();

        ConceptProto.Rule.When.Res response = runMethod(method).getRuleWhenRes();
        switch (response.getResCase()) {
            case NULL:
                return null;
            case PATTERN:
                return Graql.parser().parsePattern(response.getPattern());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Nullable
    @Override
    public final Pattern then() {
        ConceptProto.Method.Req method = ConceptProto.Method.Req.newBuilder()
                .setRuleThenReq(ConceptProto.Rule.Then.Req.getDefaultInstance()).build();

        ConceptProto.Rule.Then.Res response = runMethod(method).getRuleThenRes();
        switch (response.getResCase()) {
            case NULL:
                return null;
            case PATTERN:
                return Graql.parser().parsePattern(response.getPattern());
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    @Override
    public final Stream<Type> whenTypes() {
        throw new UnsupportedOperationException(); // TODO: remove from API
    }

    @Override
    public final Stream<Type> thenTypes() {
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
