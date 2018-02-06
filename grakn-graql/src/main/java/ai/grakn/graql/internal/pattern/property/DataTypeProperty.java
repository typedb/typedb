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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.concept.AttributeType;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.parser.QueryParserImpl;
import ai.grakn.graql.internal.reasoner.atom.property.DataTypeAtom;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code datatype} property on a {@link AttributeType}.
 *
 * This property can be queried or inserted.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class DataTypeProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    public static final String NAME = "datatype";

    public static DataTypeProperty of(AttributeType.DataType<?> datatype) {
        return new AutoValue_DataTypeProperty(datatype);
    }

    public abstract AttributeType.DataType<?> dataType();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return QueryParserImpl.DATA_TYPES.inverse().get(dataType());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.dataType(this, start, dataType()));
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).dataType(dataType());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
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
        return ImmutableSet.of(PropertyExecutor.builder(executor -> {}).build());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return new DataTypeAtom(var.var(), this, parent);
    }
}
