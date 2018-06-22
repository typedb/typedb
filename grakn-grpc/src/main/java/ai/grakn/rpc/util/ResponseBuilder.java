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

package ai.grakn.rpc.util;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.ConceptResponse;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;

/**
 * A utility class to build RPC Responses from a provided set of Grakn concepts.
 *
 * @author Grakn Warriors
 */
public class ResponseBuilder {

    public static TxResponse done() {
        return TxResponse.newBuilder().setDone(GrpcGrakn.Done.getDefaultInstance()).build();
    }

    public static TxResponse noResult() {
        return TxResponse.newBuilder().setNoResult(true).build();
    }

    public static TxResponse concept(Concept concept) {
        return TxResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
    }

    public static TxResponse rolePlayer(Role role, Thing player) {
        GrpcConcept.RolePlayer rolePlayer = GrpcConcept.RolePlayer.newBuilder()
                .setRole(ConceptBuilder.concept(role))
                .setPlayer(ConceptBuilder.concept(player))
                .build();
        return TxResponse.newBuilder().setRolePlayer(rolePlayer).build();
    }

    public static TxResponse answer(Object object) {
        return TxResponse.newBuilder().setAnswer(ConceptBuilder.answer(object)).build();
    }

    public static GrpcGrakn.DeleteResponse delete() {
        return GrpcGrakn.DeleteResponse.getDefaultInstance();
    }

    public static TxResponse conceptResponseWithNoResult() {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setNoResult(true).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    public static TxResponse conceptResopnseWithConcept(Concept concept) {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setConcept(ConceptBuilder.concept(concept)).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    public static TxResponse conceptResponseWithDataType(AttributeType.DataType<?> dataType) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        conceptResponse.setDataType(ConceptBuilder.dataType(dataType));
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }
    
    public static TxResponse conceptResponseWithAttributeValue(Object value) {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setAttributeValue(ConceptBuilder.attributeValue(value)).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    public static TxResponse conceptResponseWithPattern(Pattern pattern) {
        ConceptResponse.Builder conceptResponse = ConceptResponse.newBuilder();
        if (pattern != null) {
            conceptResponse.setPattern(pattern.toString());
        } else {
            conceptResponse.setNoResult(true);
        }
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }

    public static TxResponse conceptResponseWithRegex(String regex) {
        ConceptResponse conceptResponse = ConceptResponse.newBuilder().setRegex(regex).build();
        return TxResponse.newBuilder().setConceptResponse(conceptResponse).build();
    }
}
