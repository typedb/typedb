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

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.proto.GrpcConcept;
import ai.grakn.rpc.proto.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.proto.TransactionProto;
import ai.grakn.rpc.proto.TransactionProto.TxResponse;
import ai.grakn.rpc.proto.GrpcIterator;

import java.util.stream.Stream;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 */
public class ResponseBuilder {

    static TxResponse done() {
        return TxResponse.newBuilder().setDone(TransactionProto.Done.getDefaultInstance()).build();
    }

    static TxResponse noResult() {
        return TxResponse.newBuilder().setNoResult(true).build();
    }

    static TxResponse iteratorId(Stream<TxResponse> responses, TransactionService.Iterators iterators) {
        GrpcIterator.IteratorId iteratorId = iterators.add(responses.iterator());
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setIteratorId(iteratorId).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }
    
    static TxResponse concept(Concept concept) {
        return TxResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
    }

    static TxResponse rolePlayer(Role role, Thing player) {
        GrpcConcept.RolePlayer rolePlayer = GrpcConcept.RolePlayer.newBuilder()
                .setRole(ConceptBuilder.concept(role))
                .setPlayer(ConceptBuilder.concept(player))
                .build();
        return TxResponse.newBuilder().setRolePlayer(rolePlayer).build();
    }

    static TxResponse answer(Object object) {
        return TxResponse.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
    }

    static TransactionProto.DeleteResponse delete() {
        return TransactionProto.DeleteResponse.getDefaultInstance();
    }

    static TxResponse conceptResponseWithNoResult() {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setNoResult(true).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    static TxResponse conceptResopnseWithConcept(Concept concept) {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    static TxResponse conceptResponseWithDataType(AttributeType.DataType<?> dataType) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setDataType(ConceptBuilder.dataType(dataType));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }
    
    static TxResponse conceptResponseWithAttributeValue(Object value) {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setAttributeValue(ConceptBuilder.attributeValue(value)).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    static TxResponse conceptResponseWithPattern(Pattern pattern) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        if (pattern != null) {
            conceptResponse.setPattern(pattern.toString());
        } else {
            conceptResponse.setNoResult(true);
        }
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    static TxResponse conceptResponseWithRegex(String regex) {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setRegex(regex).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }
}
