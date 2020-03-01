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
import graql.lang.property.DataTypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.Set;

public class DataTypeExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final DataTypeProperty property;
    private final AttributeType.DataType dataType;
    private static final ImmutableMap<Graql.Token.DataType, AttributeType.DataType<?>> DATA_TYPES = dataTypes();

    DataTypeExecutor(Variable var, DataTypeProperty property) {
        if (var == null) {
            throw new NullPointerException("Variable is null");
        }
        this.var = var;

        if (property == null) {
            throw new NullPointerException("Property is null");
        }
        this.property = property;

        if (!DATA_TYPES.containsKey(property.dataType())) {
            throw new IllegalArgumentException("Unrecognised Attribute data type");
        }
        this.dataType = DATA_TYPES.get(property.dataType());
    }

    private static ImmutableMap<Graql.Token.DataType, AttributeType.DataType<?>> dataTypes() {
        ImmutableMap.Builder<Graql.Token.DataType, AttributeType.DataType<?>> dataTypes = new ImmutableMap.Builder<>();
        dataTypes.put(Graql.Token.DataType.BOOLEAN, AttributeType.DataType.BOOLEAN);
        dataTypes.put(Graql.Token.DataType.DATE, AttributeType.DataType.DATE);
        dataTypes.put(Graql.Token.DataType.DOUBLE, AttributeType.DataType.DOUBLE);
        dataTypes.put(Graql.Token.DataType.LONG, AttributeType.DataType.LONG);
        dataTypes.put(Graql.Token.DataType.STRING, AttributeType.DataType.STRING);

        return dataTypes.build();
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return Collections.unmodifiableSet(Collections.singleton(
                EquivalentFragmentSets.dataType(property, var, dataType)
        ));
    }


    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineDataType());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineDataType());
    }

    private abstract class DataTypeWriter {

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

    private class DefineDataType extends DataTypeWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            executor.getBuilder(var).dataType(dataType);
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(var);
        }
    }

    private class UndefineDataType extends DataTypeWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }

        @Override
        public void execute(WriteExecutor executor) {
            // TODO: resolve the below issue correctly
            // undefine for datatype must be supported, because it is supported in define.
            // However, making it do the right thing is difficult. Ideally we want the same as define:
            //
            //    undefine name datatype string, sub attribute; <- Remove `name`
            //    undefine first-name sub name;                 <- Remove `first-name`
            //    undefine name datatype string;                <- FAIL
            //    undefine name sub attribute;                  <- FAIL
            //
            // Doing this is tough because it means the `datatype` property needs to be aware of the context somehow.
            // As a compromise, we make all the cases succeed (where some do nothing)
        }
    }
}
