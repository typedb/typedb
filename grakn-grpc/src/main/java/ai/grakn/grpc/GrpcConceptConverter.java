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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.grpc;

import ai.grakn.concept.Concept;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.OptionalConcept;

import java.util.Optional;

/**
 * @author Felix Chapman
 */
public interface GrpcConceptConverter {
    Concept convert(GrpcConcept.Concept concept);

    default Optional<Concept> convert(OptionalConcept concept) {
        switch (concept.getValueCase()) {
            case PRESENT:
                return Optional.of(convert(concept.getPresent()));
            case ABSENT:
                return Optional.empty();
            default:
            case VALUE_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + concept);
        }
    }

    default RolePlayer convert(GrpcConcept.RolePlayer rolePlayer) {
        return RolePlayer.create(convert(rolePlayer.getRole()).asRole(), convert(rolePlayer.getPlayer()).asThing());
    }
}
