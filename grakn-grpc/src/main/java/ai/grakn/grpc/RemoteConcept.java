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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc;

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import com.google.auto.value.AutoValue;

/**
 * @author Felix Chapman
 */
@AutoValue
abstract class RemoteConcept implements Concept {

    public static RemoteConcept create(ConceptId id) {
        return new AutoValue_RemoteConcept(id);
    }

    @Override
    public abstract ConceptId getId();

    @Override
    public final Keyspace keyspace() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public final void delete() throws GraknTxOperationException {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public final boolean isDeleted() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public int compareTo(Concept concept) {
        return getId().compareTo(concept.getId());
    }
}
