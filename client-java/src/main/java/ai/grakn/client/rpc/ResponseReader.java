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

package ai.grakn.client.rpc;

import ai.grakn.client.Grakn;
import ai.grakn.client.concept.RemoteConcept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.Answer;
import ai.grakn.graql.answer.ConceptList;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.ConceptSet;
import ai.grakn.graql.answer.ConceptSetMeasure;
import ai.grakn.graql.answer.Value;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.rpc.proto.AnswerProto;
import com.google.common.collect.ImmutableMap;

import java.text.NumberFormat;
import java.text.ParseException;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * An RPC Response reader class to convert {@link AnswerProto} messages into Graql {@link Answer}s.
 */
public class ResponseReader {

    public static Answer answer(AnswerProto.Answer res, Grakn.Transaction tx) {
        switch (res.getAnswerCase()) {
            case CONCEPTMAP:
                return conceptMap(res.getConceptMap(), tx);
            case CONCEPTLIST:
                return conceptList(res.getConceptList());
            case CONCEPTSET:
                return conceptSet(res.getConceptSet());
            case CONCEPTSETMEASURE:
                return conceptSetMeasure(res.getConceptSetMeasure());
            case VALUE:
                return value(res.getValue());
            default:
            case ANSWER_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + res);
        }
    }

    static ConceptMap conceptMap(AnswerProto.ConceptMap res, Grakn.Transaction tx) {
        ImmutableMap.Builder<Var, ai.grakn.concept.Concept> map = ImmutableMap.builder();
        res.getMapMap().forEach((resVar, resConcept) -> {
            map.put(Graql.var(resVar), RemoteConcept.of(resConcept, tx));
        });
        return new ConceptMapImpl(map.build());
    }

    static ConceptList conceptList(AnswerProto.ConceptList res) {
        return new ConceptList(res.getList().getIdsList().stream().map(ConceptId::of).collect(toList()));
    }

    static ConceptSet conceptSet(AnswerProto.ConceptSet res) {
        return new ConceptSet(res.getSet().getIdsList().stream().map(ConceptId::of).collect(toSet()));
    }

    static ConceptSetMeasure conceptSetMeasure(AnswerProto.ConceptSetMeasure res) {
        return new ConceptSetMeasure(
                res.getSet().getIdsList().stream().map(ConceptId::of).collect(toSet()),
                number(res.getMeasurement())
        );
    }

    static Value value(AnswerProto.Value res) {
        return new Value(number(res.getNumber()));
    }

    static Number number(AnswerProto.Number res) {
        try {
            return NumberFormat.getInstance().parse(res.getValue());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
