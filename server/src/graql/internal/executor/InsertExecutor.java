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

package grakn.core.graql.internal.executor;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.property.HasAttributeExecutor;
import grakn.core.graql.internal.executor.property.IdExecutor;
import grakn.core.graql.internal.executor.property.IsaAbstractExecutor;
import grakn.core.graql.internal.executor.property.LabelExecutor;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.internal.executor.property.RelationExecutor;
import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsaAbstractProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;
import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("Duplicates")
public class InsertExecutor {

    private final TransactionOLTP transaction;
    private final boolean infer;

    public InsertExecutor(TransactionOLTP transaction, boolean infer) {
        this.transaction = transaction;
        this.infer = infer;
    }

    public Stream<ConceptMap> insert(InsertQuery query) {
        Collection<Statement> statements = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        if (query.match() != null) {
            MatchClause match = query.match();
            Set<Variable> matchVars = match.getSelectedNames();
            Set<Variable> insertVars = statements.stream().map(statement -> statement.var()).collect(toImmutableSet());

            Set<Variable> projectedVars = new HashSet<>(matchVars);
            projectedVars.retainAll(insertVars);

            Stream<ConceptMap> answers = transaction.stream(match.get(projectedVars), infer);
            return answers.map(answer -> insert(statements, answer)).collect(toList()).stream();
        } else {
            return Stream.of(insert(statements, new ConceptMap()));
        }
    }

    private ConceptMap insert(Collection<Statement> statements, ConceptMap results) {
        ImmutableSet.Builder<PropertyExecutor.WriteExecutor> executors = ImmutableSet.builder();
        for (Statement statement : statements) {
            for (VarProperty property : statement.properties()){
                executors.addAll(insertable(statement.var(), property).insertExecutors());
            }
        }
        return Writer.create(executors.build(), transaction).write(results);
    }

    private PropertyExecutor.Insertable insertable(Variable var, VarProperty property) {
        if (property instanceof IsaAbstractProperty) {
            return new IsaAbstractExecutor(var, (IsaAbstractProperty) property);

        } else if (property instanceof HasAttributeProperty) {
            return new HasAttributeExecutor(var, (HasAttributeProperty) property);

        } else if (property instanceof IdProperty) {
            return new IdExecutor(var, (IdProperty) property);

        } else if (property instanceof LabelProperty) {
            return new LabelExecutor(var, (LabelProperty) property);

        } else if (property instanceof RelationProperty) {
            return new RelationExecutor(var, (RelationProperty) property);

        } else if (property instanceof ValueProperty) {
            return new ValueExecutor(var, (ValueProperty) property);

        } else {
            throw GraqlQueryException.insertUnsupportedProperty(property.getName());
        }
    }
}
