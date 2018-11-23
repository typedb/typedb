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

package grakn.core.client.rpc;

import grakn.core.client.Grakn;
import grakn.core.client.concept.RemoteConcept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Var;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptList;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.ConceptSetMeasure;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.protocol.AnswerProto;
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
            case ANSWERGROUP:
                return answerGroup(res.getAnswerGroup(), tx);
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

    static AnswerGroup<?> answerGroup(AnswerProto.AnswerGroup res, Grakn.Transaction tx) {
        return new AnswerGroup<>(
                RemoteConcept.of(res.getOwner(), tx),
                res.getAnswersList().stream().map(answer -> answer(answer, tx)).collect(toList())
        );
    }
    static ConceptMap conceptMap(AnswerProto.ConceptMap res, Grakn.Transaction tx) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();
        res.getMapMap().forEach((resVar, resConcept) -> {
            map.put(Graql.var(resVar), RemoteConcept.of(resConcept, tx));
        });
        return new ConceptMap(map.build());
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
