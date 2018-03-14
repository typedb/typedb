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

package ai.grakn.remote.concept;

import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.remote.RemoteGraknTx;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * @author Felix Chapman
 */
abstract class RemoteConcept<Self extends Concept> implements Concept {

    abstract RemoteGraknTx tx();

    @Override
    public abstract ConceptId getId();

    @Override
    public final Keyspace keyspace() {
        return tx().keyspace();
    }

    @Override
    public final void delete() throws GraknTxOperationException {
        runVoidMethod(ConceptMethod.DELETE);
    }

    @Override
    public final boolean isDeleted() {
        return tx().client().getConcept(getId()) == null;
    }

    protected final <T> T runMethod(ConceptMethod<T> property) {
        return Objects.requireNonNull(runNullableMethod(property));
    }

    @Nullable
    private <T> T runNullableMethod(ConceptMethod<T> property) {
        return tx().client().runConceptMethod(getId(), property);
    }

    final Self runVoidMethod(ConceptMethod<Void> property) {
        runNullableMethod(property);
        return asSelf(this);
    }

    abstract Self asSelf(Concept concept);
}
