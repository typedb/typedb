/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.executor.property;

import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import graql.lang.property.AbstractProperty;
import graql.lang.property.DataTypeProperty;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.IdProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.NeqProperty;
import graql.lang.property.PlaysProperty;
import graql.lang.property.RegexProperty;
import graql.lang.property.RelatesProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.SubProperty;
import graql.lang.property.ThenProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.property.WhenProperty;
import graql.lang.statement.Variable;

public class PropertyExecutorFactoryImpl implements PropertyExecutorFactory {

    public PropertyExecutor.Definable definable(Variable var, VarProperty property) {
        PropertyExecutorFactory propertyExecutorFactory = new PropertyExecutorFactoryImpl();
        PropertyExecutor executor = propertyExecutorFactory.create(var, property);

        if (executor instanceof PropertyExecutor.Definable) {
            return (PropertyExecutor.Definable) executor;
        } else {
            throw GraqlSemanticException.defineUnsupportedProperty(property.keyword());
        }
    }

    public PropertyExecutor.Insertable insertable(Variable var, VarProperty property) {
        PropertyExecutorFactory propertyExecutorFactory = new PropertyExecutorFactoryImpl();
        PropertyExecutor executor = propertyExecutorFactory.create(var, property);

        if (executor instanceof PropertyExecutor.Insertable) {
            return (PropertyExecutor.Insertable) executor;
        } else {
            throw GraqlSemanticException.insertUnsupportedProperty(property.keyword());
        }
    }

    public PropertyExecutor create(Variable var, VarProperty property) {
        if (property instanceof DataTypeProperty) {
            return new DataTypeExecutor(var, (DataTypeProperty) property);

        } else if (property instanceof HasAttributeProperty) {
            return new HasAttributeExecutor(var, (HasAttributeProperty) property);

        } else if (property instanceof HasAttributeTypeProperty) {
            return new HasAttributeTypeExecutor(var, (HasAttributeTypeProperty) property, this);

        } else if (property instanceof IdProperty) {
            return new IdExecutor(var, (IdProperty) property);

        } else if (property instanceof AbstractProperty) {
            return new AbstractExecutor(var, (AbstractProperty) property);

        } else if (property instanceof IsaProperty) {
            return new IsaExecutor(var, (IsaProperty) property);

        } else if (property instanceof TypeProperty) {
            return new TypeExecutor(var, (TypeProperty) property);

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
