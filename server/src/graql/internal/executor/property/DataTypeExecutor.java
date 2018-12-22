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

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.property.DataTypeAtom;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.Set;

public class DataTypeExecutor implements PropertyExecutor.Definable,
                                         PropertyExecutor.Matchable,
                                         PropertyExecutor.Atomable {

    private final Variable var;
    private final DataTypeProperty property;

    DataTypeExecutor(Variable var, DataTypeProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineDataType());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineDataType());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return Collections.unmodifiableSet(Collections.singleton(
                EquivalentFragmentSets.dataType(property, var, property.dataType())
        ));
    }

    public DataTypeAtom atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        return DataTypeAtom.create(var, property, parent);
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
            executor.getBuilder(var).dataType(property.dataType());
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
