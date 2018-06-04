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

import ai.grakn.concept.Concept;
import ai.grakn.rpc.GrpcClient;
import ai.grakn.rpc.ResponseIterator;
import ai.grakn.rpc.RolePlayer;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.OptionalConcept;
import ai.grakn.rpc.generated.GrpcIterator;

import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Grakn Warriors
 */
public abstract class TxConceptReader {
    public abstract Concept concept(GrpcConcept.Concept concept);

    public Optional<Concept> optionalConcept(OptionalConcept concept) {
        switch (concept.getValueCase()) {
            case PRESENT:
                return Optional.of(concept(concept.getPresent()));
            case ABSENT:
                return Optional.empty();
            default:
            case VALUE_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }

    public RolePlayer rolePlayer(GrpcConcept.RolePlayer rolePlayer) {
        return RolePlayer.create(concept(rolePlayer.getRole()).asRole(), concept(rolePlayer.getPlayer()).asThing());
    }

    public <T> T conceptResponse(GrpcConcept.ConceptMethod rpcMethod, GrpcConcept.ConceptResponse conceptResponse, GrpcClient client) {
        switch (rpcMethod.getConceptMethodCase()) {
            case GETATTRIBUTETYPES:
            case GETKEYTYPES:
            case GETSUPERCONCEPTS:
            case GETSUBCONCEPTS:
            case GETATTRIBUTES:
            case GETKEYS:
            case GETROLESPLAYEDBYTYPE:
            case GETINSTANCES:
            case GETROLESPLAYEDBYTHING:
            case GETRELATIONSHIPS:
            case GETRELATIONSHIPTYPESTHATRELATEROLE:
            case GETTYPESTHATPLAYROLE:
            case GETRELATEDROLES:
            case GETOWNERS:
            case GETATTRIBUTESBYTYPES:
            case GETKEYSBYTYPES:
            case GETROLEPLAYERSBYROLES:
            case GETRELATIONSHIPSBYROLES:
                return (T) conceptsStream(conceptResponse.getIteratorId(), client);
            case GETROLEPLAYERS:
                return (T) rolePlayersStream(conceptResponse.getIteratorId(), client);
        }

        throw new IllegalArgumentException("Unrecognised " + conceptResponse);
    }

    public Stream<? extends Concept> conceptsStream(GrpcIterator.IteratorId iteratorId, GrpcClient client) {
        Iterable<? extends Concept> iterable = () -> new ResponseIterator<>(client, iteratorId, response -> concept(response.getConcept()));
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public Stream<RolePlayer> rolePlayersStream(GrpcIterator.IteratorId iteratorId, GrpcClient client) {
        Iterable<RolePlayer> iterable = () -> new ResponseIterator<>(client, iteratorId, response -> rolePlayer(response.getRolePlayer()));
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
