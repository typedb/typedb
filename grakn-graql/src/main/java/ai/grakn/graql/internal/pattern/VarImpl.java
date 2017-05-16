/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
 *
 */

package ai.grakn.graql.internal.pattern;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import org.apache.commons.lang.StringUtils;

import java.util.function.Function;

/**
 *
 * @author Felix Chapman
 */
final class VarImpl implements Var {
    private final String value;

    VarImpl(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Var map(Function<String, String> mapper) {
        return Graql.varName(mapper.apply(value));
    }

    @Override
    public VarPattern pattern() {
        return Patterns.var(this);
    }

    @Override
    public String shortName() {
        return "$" + StringUtils.left(value, 3);
    }

    @Override
    public String toString() {
        return "$" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarImpl varName = (VarImpl) o;

        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public VarPattern id(ConceptId id) {
        return pattern().id(id);
    }

    @Override
    public VarPattern label(String label) {
        return pattern().label(label);
    }

    @Override
    public VarPattern label(TypeLabel label) {
        return pattern().label(label);
    }

    @Override
    public VarPattern val(Object value) {
        return pattern().val(value);
    }

    @Override
    public VarPattern val(ValuePredicate predicate) {
        return pattern().val(predicate);
    }

    @Override
    public VarPattern has(String type, Object value) {
        return pattern().has(type, value);
    }

    @Override
    public VarPattern has(String type, ValuePredicate predicate) {
        return pattern().has(type, predicate);
    }

    @Override
    public VarPattern has(String type, VarPattern var) {
        return pattern().has(type, var);
    }

    @Override
    public VarPattern has(TypeLabel type, VarPattern var) {
        return pattern().has(type, var);
    }

    @Override
    public VarPattern isa(String type) {
        return pattern().isa(type);
    }

    @Override
    public VarPattern isa(VarPattern type) {
        return pattern().isa(type);
    }

    @Override
    public VarPattern sub(String type) {
        return pattern().sub(type);
    }

    @Override
    public VarPattern sub(VarPattern type) {
        return pattern().sub(type);
    }

    @Override
    public VarPattern relates(String type) {
        return pattern().relates(type);
    }

    @Override
    public VarPattern relates(VarPattern type) {
        return pattern().relates(type);
    }

    @Override
    public VarPattern plays(String type) {
        return pattern().plays(type);
    }

    @Override
    public VarPattern plays(VarPattern type) {
        return pattern().plays(type);
    }

    @Override
    public VarPattern hasScope(VarPattern type) {
        return pattern().hasScope(type);
    }

    @Override
    public VarPattern has(String type) {
        return pattern().has(type);
    }

    @Override
    public VarPattern has(VarPattern type) {
        return pattern().has(type);
    }

    @Override
    public VarPattern key(String type) {
        return pattern().key(type);
    }

    @Override
    public VarPattern key(VarPattern type) {
        return pattern().key(type);
    }

    @Override
    public VarPattern rel(String roleplayer) {
        return pattern().rel(roleplayer);
    }

    @Override
    public VarPattern rel(VarPattern roleplayer) {
        return pattern().rel(roleplayer);
    }

    @Override
    public VarPattern rel(String roletype, String roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern rel(VarPattern roletype, String roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern rel(String roletype, VarPattern roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern rel(VarPattern roletype, VarPattern roleplayer) {
        return pattern().rel(roletype, roleplayer);
    }

    @Override
    public VarPattern isAbstract() {
        return pattern().isAbstract();
    }

    @Override
    public VarPattern datatype(ResourceType.DataType<?> datatype) {
        return pattern().datatype(datatype);
    }

    @Override
    public VarPattern regex(String regex) {
        return pattern().regex(regex);
    }

    @Override
    public VarPattern lhs(Pattern lhs) {
        return pattern().lhs(lhs);
    }

    @Override
    public VarPattern rhs(Pattern rhs) {
        return pattern().rhs(rhs);
    }

    @Override
    public VarPattern neq(String varName) {
        return pattern().neq(varName);
    }

    @Override
    public VarPattern neq(VarPattern var) {
        return pattern().neq(var);
    }
}
