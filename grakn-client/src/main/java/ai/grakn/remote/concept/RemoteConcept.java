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
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.remote.RemoteGraknTx;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.ask;
import static ai.grakn.graql.Graql.var;

/**
 * @author Felix Chapman
 */
abstract class RemoteConcept implements Concept {

    static final Var ME = var("me");
    static final Var TARGET = var("target");

    abstract RemoteGraknTx tx();

    @Override
    public abstract ConceptId getId();

    @Override
    public final Keyspace keyspace() {
        return tx().keyspace();
    }

    @Override
    public final void delete() throws GraknTxOperationException {
        throw new UnsupportedOperationException(); // TODO: implement
    }

    @Override
    public final boolean isDeleted() {
        return !tx().graql().match(var().id(getId())).aggregate(ask()).execute();
    }

    protected final <T> T getProperty(ConceptProperty<T> property) {
        return tx().client().getConceptProperty(getId(), property);
    }

    protected final Stream<Concept> query(Pattern... patterns) {
        return queryAnswers(patterns).map(answer -> answer.get(TARGET));
    }

    protected final Stream<Answer> queryAnswers(Pattern... patterns) {
        Pattern myId = ME.id(getId());

        Collection<Pattern> patternCollection = ImmutableList.<Pattern>builder().add(myId).add(patterns).build();

        return tx().graql().match(patternCollection).get().stream();
    }
}
