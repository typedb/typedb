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

package grakn.core.graql.internal.executor.property;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.SubExplicitProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;

import java.util.Set;

// TODO: The exceptions in this class needs to be tidied up
public interface PropertyExecutor {

    interface Definable extends PropertyExecutor {

        Set<Writer> defineExecutors();

        Set<Writer> undefineExecutors();
    }

    interface Insertable extends PropertyExecutor {

        Set<Writer> insertExecutors();
    }

    interface Matchable extends PropertyExecutor {

        Set<EquivalentFragmentSet> matchFragments();
    }

    interface Atomable extends PropertyExecutor {

        Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements);
    }

    interface Writer {

        Variable var();

        VarProperty property();

        Set<Variable> requiredVars();

        Set<Variable> producedVars();

        void execute(WriteExecutor executor);
    }

    static Definable definable(Variable var, VarProperty property) {
        PropertyExecutor executor = propertyExecutor(var, property);

        if (executor instanceof Definable) {
            return (Definable) executor;
        } else {
            throw GraqlQueryException.defineUnsupportedProperty(property.name());
        }
    }

    static Insertable insertable(Variable var, VarProperty property) {
        PropertyExecutor executor = propertyExecutor(var, property);

        if (executor instanceof Insertable) {
            return (Insertable) executor;
        } else {
            throw GraqlQueryException.insertUnsupportedProperty(property.name());
        }
    }

    static Matchable matchable(Variable var, VarProperty property) {
        PropertyExecutor executor = propertyExecutor(var, property);

        if (executor instanceof Matchable) {
            return (Matchable) executor;
        } else {
            throw new UnsupportedOperationException(ErrorMessage.MATCH_INVALID.getMessage(property.name()));
        }
    }

    static Atomable atomable(Variable var, VarProperty property) {
        PropertyExecutor executor = propertyExecutor(var, property);

        if (executor instanceof Atomable) {
            return (Atomable) executor;
        } else {
            throw new IllegalArgumentException("Unrecognised subclass of " + VarProperty.class.getName());
        }
    }

    static PropertyExecutor propertyExecutor(Variable var, VarProperty property) {
        if (property instanceof DataTypeProperty) {
            return new DataTypeExecutor(var, (DataTypeProperty) property);

        } else if (property instanceof HasAttributeProperty) {
            return new HasAttributeExecutor(var, (HasAttributeProperty) property);

        } else if (property instanceof HasAttributeTypeProperty) {
            return new HasAttributeTypeExecutor(var, (HasAttributeTypeProperty) property);

        } else if (property instanceof IdProperty) {
            return new IdExecutor(var, (IdProperty) property);

        } else if (property instanceof IsAbstractProperty) {
            return new IsAbstractExecutor(var, (IsAbstractProperty) property);

        } else if (property instanceof IsaExplicitProperty) {
            return new IsaExecutor.IsaExplicitExecutor(var, (IsaExplicitProperty) property);

        } else if (property instanceof IsaProperty) {
            return new IsaExecutor(var, (IsaProperty) property);

        } else if (property instanceof LabelProperty) {
            return new LabelExecutor(var, (LabelProperty) property);

        } else if (property instanceof NeqProperty) {
            return new NeqExecutor(var, (NeqProperty) property);

        } else if (property instanceof PlaysProperty) {
            return new PlaysExecutor(var, (PlaysProperty) property);

        } else if (property instanceof RegexProperty) {
            return new RegexExecutor(var, (RegexProperty) property);

        } else if (property instanceof RelatesProperty) {
            return new RelatesExecutor(var, (RelatesProperty) property);

        } else if (property instanceof RelationProperty) {
            return new RelationExecutor(var, (RelationProperty) property);

        } else if (property instanceof SubExplicitProperty) {
            return new SubExecutor.SubExplicitExecutor(var, (SubExplicitProperty) property);

        } else if (property instanceof SubProperty) {
            return new SubExecutor(var, (SubProperty) property);

        } else if (property instanceof ValueProperty) {
            return new ValueExecutor(var, (ValueProperty) property);

        } else if (property instanceof ThenProperty) {
            return new ThenExecutor(var, (ThenProperty) property);

        } else if (property instanceof WhenProperty) {
            return new WhenExecutor(var, (WhenProperty) property);

        } else {
            throw new IllegalArgumentException("Unrecognised subclass of PropertyExecutor");
        }
    }
}
