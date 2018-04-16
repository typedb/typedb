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

package ai.grakn.graql.internal.query;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTx;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.DeleteQueryAdmin;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link DeleteQuery} that will execute deletions for every result of a {@link Match}
 */
@AutoValue
abstract class DeleteQueryImpl extends AbstractQuery<Void, Void> implements DeleteQueryAdmin {

    /**
     * @param vars a collection of variables to delete
     * @param match a pattern to match and delete for each result
     */
    static DeleteQueryImpl of(Collection<? extends Var> vars, Match match) {
        return new AutoValue_DeleteQueryImpl(match, ImmutableSet.copyOf(vars));
    }

    @Override
    public final Void execute() {
        queryRunner().run(this);
        return null;
    }

    @Override
    public final boolean isReadOnly() {
        return false;
    }

    @Override
    public final Optional<? extends GraknTx> tx() {
        return match().admin().tx();
    }

    @Override
    public DeleteQuery withTx(GraknTx tx) {
        return Queries.delete(vars(), match().withTx(tx));
    }

    @Override
    public DeleteQueryAdmin admin() {
        return this;
    }

    @Override
    public String toString() {
        return match() + " delete " + vars().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim();
    }

    @Override
    protected final Stream<Void> stream() {
        execute();
        return Stream.empty();
    }

    @Nullable
    @Override
    public final Boolean inferring() {
        return match().admin().inferring();
    }
}
