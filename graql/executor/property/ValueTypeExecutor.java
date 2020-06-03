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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.Graql;
import graql.lang.property.ValueTypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.Set;

public class ValueTypeExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final ValueTypeProperty property;
    private final AttributeType.ValueType valueType;
    private static final ImmutableMap<Graql.Token.ValueType, AttributeType.ValueType<?>> VALUE_TYPES = valueTypes();

    ValueTypeExecutor(Variable var, ValueTypeProperty property) {
        if (var == null) {
            throw new NullPointerException("Variable is null");
        }
        this.var = var;

        if (property == null) {
            throw new NullPointerException("Property is null");
        }
        this.property = property;

        if (!VALUE_TYPES.containsKey(property.ValueType())) {
            throw new IllegalArgumentException("Unrecognised Attribute value type");
        }
        this.valueType = VALUE_TYPES.get(property.ValueType());
    }

    private static ImmutableMap<Graql.Token.ValueType, AttributeType.ValueType<?>> valueTypes() {
        ImmutableMap.Builder<Graql.Token.ValueType, AttributeType.ValueType<?>> valueTypes = new ImmutableMap.Builder<>();
        valueTypes.put(Graql.Token.ValueType.BOOLEAN, AttributeType.ValueType.BOOLEAN);
        valueTypes.put(Graql.Token.ValueType.DATETIME, AttributeType.ValueType.DATETIME);
        valueTypes.put(Graql.Token.ValueType.DOUBLE, AttributeType.ValueType.DOUBLE);
        valueTypes.put(Graql.Token.ValueType.LONG, AttributeType.ValueType.LONG);
        valueTypes.put(Graql.Token.ValueType.STRING, AttributeType.ValueType.STRING);

        return valueTypes.build();
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return Collections.unmodifiableSet(Collections.singleton(
                EquivalentFragmentSets.valueType(property, var, valueType)
        ));
    }


    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineValueType());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineValueType());
    }

    private abstract class ValueTypeWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            return ImmutableSet.of();
        }
    }

    private class DefineValueType extends ValueTypeWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            executor.getBuilder(var).valueType(valueType);
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(var);
        }
    }

    private class UndefineValueType extends ValueTypeWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }

        @Override
        public void execute(WriteExecutor executor) {
            // TODO: resolve the below issue correctly
            // undefine for valueType must be supported, because it is supported in define.
            // However, making it do the right thing is difficult. Ideally we want the same as define:
            //
            //    undefine name valueType string, sub attribute; <- Remove `name`
            //    undefine first-name sub name;                 <- Remove `first-name`
            //    undefine name valueType string;                <- FAIL
            //    undefine name sub attribute;                  <- FAIL
            //
            // Doing this is tough because it means the `valueType` property needs to be aware of the context somehow.
            // As a compromise, we make all the cases succeed (where some do nothing)
        }
    }
}
