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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.property.DataTypeExecutor;
import grakn.core.graql.internal.executor.property.HasAttributeTypeExecutor;
import grakn.core.graql.internal.executor.property.IdExecutor;
import grakn.core.graql.internal.executor.property.IsAbstractExecutor;
import grakn.core.graql.internal.executor.property.LabelExecutor;
import grakn.core.graql.internal.executor.property.PlaysExecutor;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.internal.executor.property.RegexExecutor;
import grakn.core.graql.internal.executor.property.RelatesExecutor;
import grakn.core.graql.internal.executor.property.SubAbstractExecutor;
import grakn.core.graql.query.UndefineQuery;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.SubAbstractProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.session.TransactionOLTP;

import static grakn.core.common.util.CommonUtil.toImmutableList;

@SuppressWarnings("Duplicates")
public class UndefineExecutor {

    private final TransactionOLTP transaction;

    public UndefineExecutor(TransactionOLTP transaction) {
        this.transaction = transaction;
    }

    public ConceptMap undefine(UndefineQuery query) {
        ImmutableSet.Builder<PropertyExecutor.WriteExecutor> executors = ImmutableSet.builder();
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : allPatterns) {
            for (VarProperty property : statement.properties()){
                executors.addAll(definable(statement.var(), property).undefineExecutors());
            }
        }
        return Writer.create(executors.build(), transaction).write(new ConceptMap());
    }

    private PropertyExecutor.Definable definable(Variable var, VarProperty property) {
        if (property instanceof SubAbstractProperty) {
            return new SubAbstractExecutor(var, (SubAbstractProperty) property);

        } else if (property instanceof DataTypeProperty) {
            return new DataTypeExecutor(var, (DataTypeProperty) property);

        } else if (property instanceof HasAttributeTypeProperty) {
            return new HasAttributeTypeExecutor(var, (HasAttributeTypeProperty) property);

        } else if (property instanceof IdProperty) {
            return new IdExecutor(var, (IdProperty) property);

        } else if (property instanceof IsAbstractProperty) {
            return new IsAbstractExecutor(var, (IsAbstractProperty) property);

        } else if (property instanceof LabelProperty) {
            return new LabelExecutor(var, (LabelProperty) property);

        } else if (property instanceof PlaysProperty) {
            return new PlaysExecutor(var, (PlaysProperty) property);

        } else if (property instanceof RegexProperty) {
            return new RegexExecutor(var, (RegexProperty) property);

        } else if (property instanceof RelatesProperty) {
            return new RelatesExecutor(var, (RelatesProperty) property);

        } else {
            throw GraqlQueryException.defineUnsupportedProperty(property.getName());
        }
    }
}
