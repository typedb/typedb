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

package ai.grakn.engine.rpc;

import ai.grakn.concept.Concept;
import ai.grakn.graql.GraqlConverter;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.QueryResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Converts to GRPC result format. This is a special format for {@link Answer}s, but JSON strings for everything else.
 *
 * @author Felix Chapman
 */
class GrpcConverter implements GraqlConverter<Object, QueryResult> {

    private GrpcConverter() {

    }

    public static GrpcConverter get() {
        return new GrpcConverter();
    }

    @Override
    public QueryResult complete(Object builder) {
        if (builder instanceof GrpcGrakn.Answer) {
            return QueryResult.newBuilder().setAnswer((GrpcGrakn.Answer) builder).build();
        } else {
            // If not an answer, convert to JSON
            return QueryResult.newBuilder().setOtherResult(Printers.json().convert(builder)).build();
        }
    }

    @Override
    public Concept build(Concept concept) {
        return concept;
    }

    @Override
    public Boolean build(boolean bool) {
        return bool;
    }

    @Override
    public Optional<?> build(Optional<?> optional) {
        return optional;
    }

    @Override
    public Collection<?> build(Collection<?> collection) {
        return collection;
    }

    @Override
    public Map<?, ?> build(Map<?, ?> map) {
        return map;
    }

    @Override
    public GrpcGrakn.Answer build(Answer answer) {
        GrpcGrakn.Answer.Builder answerRps = GrpcGrakn.Answer.newBuilder();
        answer.forEach((var, concept) -> {
            GrpcConcept.Concept conceptRps = GrpcUtil.convert(concept);
            answerRps.putAnswer(var.getValue(), conceptRps);
        });
        return answerRps.build();
    }

    @Override
    public Object buildDefault(Object object) {
        return object;
    }
}
