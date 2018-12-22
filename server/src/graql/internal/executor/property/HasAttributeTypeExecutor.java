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
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HasAttributeTypeExecutor implements PropertyExecutor.Definable, PropertyExecutor.Matchable {

    private final Variable var;
    private final HasAttributeTypeProperty property;

    public HasAttributeTypeExecutor(Variable var, HasAttributeTypeProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineHasAttributeType());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineHasAttributeType());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Set<EquivalentFragmentSet> fragments = new HashSet<>();

        PlaysExecutor playsOwnerExecutor = new PlaysExecutor(var, new PlaysProperty(property.ownerRole(), property.isRequired()));
        //TODO: Get this to use real constraints no just the required flag
        PlaysExecutor playsValueExecutor = new PlaysExecutor(property.attributeType().var(), new PlaysProperty(property.valueRole(), false));
        NeqExecutor neqExecutor = new NeqExecutor(property.valueRole().var(), new NeqProperty(property.ownerRole()));

        fragments.addAll(playsOwnerExecutor.matchFragments());
        fragments.addAll(playsValueExecutor.matchFragments());
        fragments.addAll(neqExecutor.matchFragments());

        return ImmutableSet.copyOf(fragments);
    }

    private abstract class HasAttributeTypeWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.attributeType().var());

            return Collections.unmodifiableSet(required);
        }

        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }
    }

    private class DefineHasAttributeType extends HasAttributeTypeWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            Type entityTypeConcept = executor.getConcept(var).asType();
            AttributeType attributeTypeConcept = executor.getConcept(property.attributeType().var()).asAttributeType();

            if (property.isRequired()) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.has(attributeTypeConcept);
            }
        }
    }

    private class UndefineHasAttributeType extends HasAttributeTypeWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            Type type = executor.getConcept(var).asType();
            AttributeType<?> attributeType = executor.getConcept(property.attributeType().var()).asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (property.isRequired()) {
                    type.unkey(attributeType);
                } else {
                    type.unhas(attributeType);
                }
            }
        }
    }
}
