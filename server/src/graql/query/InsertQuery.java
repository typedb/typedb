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

package grakn.core.graql.query;

import com.google.common.base.Preconditions;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.admin.MatchAdmin;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.server.Transaction;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A query for inserting data, which can be built from a {@link QueryBuilder} or a {@link Match}.
 * When built from a {@code QueryBuilder}, the insert query will execute once, inserting all the variables provided.
 * When built from a {@link Match}, the insert query will execute for each result of the {@link Match},
 * where variable names in the insert query are bound to the concept in the result of the {@link Match}.
 */
public class InsertQuery implements Query<ConceptMap> {

    private final Transaction tx;
    private final Match match;
    private final Collection<Statement> statements;

    /**
     * At least one of {@code tx} and {@code match} must be absent.
     *
     * @param tx         the graph to execute on
     * @param match      the {@link Match} to insert for each result
     * @param statements a collection of Vars to insert
     */
    public InsertQuery(@Nullable Transaction tx, @Nullable MatchAdmin match, Collection<Statement> statements) {
        if (match != null && match.tx() != null) Preconditions.checkArgument(match.tx().equals(tx));

        if (statements.isEmpty()) {
            throw GraqlQueryException.noPatterns();
        }

        this.tx = tx;
        this.match = match;
        this.statements = statements;
    }

    @Nullable
    @Override
    public Transaction tx() {
        return tx;
    }

    /**
     * @return the {@link Match} that this insert query is using, if it was provided one
     */
    @Nullable
    @CheckReturnValue
    public Match match() {
        return match;
    }

    /**
     * @return the variables to insert in the insert query
     */
    @CheckReturnValue
    public Collection<Statement> statements() {
        return statements;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public final Stream<ConceptMap> stream() {
        return executor().run(this);
    }

    @CheckReturnValue
    public InsertQuery admin() {
        return this;
    }

    @CheckReturnValue
    public Set<SchemaConcept> getSchemaConcepts() {
        if (getTx() == null) throw GraqlQueryException.noTx();
        Transaction theGraph = getTx();

        Set<SchemaConcept> types = allVarPatterns()
                .map(Statement::getTypeLabel)
                .flatMap(CommonUtil::optionalToStream)
                .map(theGraph::<Type>getSchemaConcept)
                .collect(Collectors.toSet());

        if (match() != null) types.addAll(match().admin().getSchemaConcepts());

        return types;
    }

    private Stream<Statement> allVarPatterns() {
        return statements().stream().flatMap(v -> v.innerStatements().stream());
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
        builder.append(statements().stream().map(v -> v + ";").collect(Collectors.joining("\n")).trim());

        return builder.toString();
    }

    @Override
    public final Boolean inferring() {
        if (match() != null) return match().admin().inferring();
        else return false;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof InsertQuery) {
            InsertQuery that = (InsertQuery) o;
            return ((this.tx == null) ? (that.tx() == null) : this.tx.equals(that.tx()))
                    && ((this.match == null) ? (that.match() == null) : this.match.equals(that.match()))
                    && (this.statements.equals(that.statements()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (tx == null) ? 0 : this.tx.hashCode();
        h *= 1000003;
        h ^= (match == null) ? 0 : this.match.hashCode();
        h *= 1000003;
        h ^= this.statements.hashCode();
        return h;
    }
}
