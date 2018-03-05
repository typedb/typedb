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
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.remote.RemoteGraknTx;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.ask;
import static ai.grakn.graql.Graql.var;

/**
 * @author Felix Chapman
 */
abstract class RemoteConcept<Self extends Concept> implements Concept {

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
        tx().graql().match(me()).delete(ME).execute();
    }

    @Override
    public final boolean isDeleted() {
        return !tx().graql().match(me()).aggregate(ask()).execute();
    }

    protected final <T> T runMethod(ConceptMethod<T> property) {
        return Objects.requireNonNull(runNullableMethod(property));
    }

    @Nullable
    protected final <T> T runNullableMethod(ConceptMethod<T> property) {
        return tx().client().runConceptMethod(getId(), property);
    }

    protected final VarPattern me() {
        return ME.id(getId());
    }

    protected final Stream<Concept> query(Pattern... patterns) {
        return queryAnswers(patterns).map(answer -> answer.get(TARGET));
    }

    protected final Stream<Answer> queryAnswers(Pattern... patterns) {
        Collection<Pattern> patternCollection = ImmutableList.<Pattern>builder().add(me()).add(patterns).build();

        return tx().graql().match(patternCollection).get().stream();
    }

    protected final Concept insert(VarPattern... patterns) {
        Collection<VarPattern> patternCollection = ImmutableList.<VarPattern>builder().add(me()).add(patterns).build();

        return Iterables.getOnlyElement(tx().graql().insert(patternCollection).execute()).get(TARGET);
    }

    abstract Self asSelf(Concept concept);
}
