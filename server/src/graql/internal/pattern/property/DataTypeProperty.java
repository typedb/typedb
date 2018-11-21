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

package grakn.core.graql.internal.pattern.property;

import grakn.core.graql.concept.AttributeType;
import grakn.core.server.exception.GraqlQueryException;
import grakn.core.graql.query.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.UniqueVarProperty;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.parser.QueryParserImpl;
import grakn.core.graql.internal.reasoner.atom.property.DataTypeAtom;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code datatype} property on a {@link AttributeType}.
 *
 * This property can be queried or inserted.
 *
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
        return DataTypeAtom.create(var.var(), this, parent);
    }
}
