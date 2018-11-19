/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.query;

import grakn.core.server.Transaction;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.InsertQuery;
import grakn.core.graql.Match;
import grakn.core.graql.admin.InsertQueryAdmin;
import grakn.core.graql.admin.MatchAdmin;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.common.util.CommonUtil;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A query that will insert a collection of variables into a graph
 *
 */
@AutoValue
abstract class InsertQueryImpl implements InsertQueryAdmin {

    /**
     * At least one of {@code tx} and {@code match} must be absent.
     * @param tx the graph to execute on
     * @param match the {@link Match} to insert for each result
     * @param vars a collection of Vars to insert
     */
    static InsertQueryImpl create(Transaction tx, MatchAdmin match, Collection<VarPatternAdmin> vars) {
        if (match != null && match.tx() != null) Preconditions.checkArgument(match.tx().equals(tx));

        if (vars.isEmpty()) {
            throw GraqlQueryException.noPatterns();
        }

        return new AutoValue_InsertQueryImpl(tx, match, ImmutableList.copyOf(vars));
    }

    @Override
    public final InsertQuery withTx(Transaction tx) {
        if (match() != null) {
            return Queries.insert(match().withTx(tx).admin(), varPatterns());
        } else {
            return Queries.insert(tx, varPatterns());
        }
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public final Stream<ConceptMap> stream() {
        return executor().run(this);
    }

    @Override
    public InsertQueryAdmin admin() {
        return this;
    }

    @Override
    public Set<SchemaConcept> getSchemaConcepts() {
        if (getTx() == null) throw GraqlQueryException.noTx();
        Transaction theGraph = getTx();

        Set<SchemaConcept> types = allVarPatterns()
                .map(VarPatternAdmin::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(theGraph::<Type>getSchemaConcept)
                .collect(Collectors.toSet());

        if (match() != null) types.addAll(match().admin().getSchemaConcepts());

        return types;
    }

    private Stream<VarPatternAdmin> allVarPatterns() {
        return varPatterns().stream().flatMap(v -> v.innerVarPatterns().stream());
    }

    private Transaction getTx() {
        if (match() != null && match().admin().tx() != null) return match().admin().tx();
        else return tx();
    }

    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder();

        if (match() != null) builder.append(match()).append("\n");
        builder.append("insert ");
        builder.append(varPatterns().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim());

        return builder.toString();
    }

    @Override
    public final Boolean inferring() {
        if (match() != null) return match().admin().inferring();
        else return false;
    }
}
