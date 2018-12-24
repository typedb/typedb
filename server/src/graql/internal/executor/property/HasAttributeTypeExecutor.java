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
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.reasoner.atom.binary.HasAtom;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HasAttributeTypeExecutor implements PropertyExecutor.Definable,
                                                 PropertyExecutor.Matchable,
                                                 PropertyExecutor.Atomable {

    private final Variable var;
    private final HasAttributeTypeProperty property;

    HasAttributeTypeExecutor(Variable var, HasAttributeTypeProperty property) {
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

        PlaysProperty playsOwnerProperty = new PlaysProperty(property.ownerRole(), property.isKey());
        PlaysExecutor playsOwnerExecutor = new PlaysExecutor(var, playsOwnerProperty);

        //TODO: Get this to use real constraints no just the required flag
        PlaysProperty playsValueProperty = new PlaysProperty(property.valueRole(), false);
        PlaysExecutor playsValueExecutor = new PlaysExecutor(property.attributeType().var(), playsValueProperty);

        NeqProperty neqProperty = new NeqProperty(property.ownerRole());
        NeqExecutor neqExecutor = new NeqExecutor(property.valueRole().var(), neqProperty);

        fragments.addAll(playsOwnerExecutor.matchFragments());
        fragments.addAll(playsValueExecutor.matchFragments());
        fragments.addAll(neqExecutor.matchFragments());

        return ImmutableSet.copyOf(fragments);
    }

    @Override
    public Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //NB: HasResourceType is a special case and it doesn't allow variables as resource types
        Variable varName = var.asUserDefined();
        Label label = property.attributeType().getTypeLabel().orElse(null);

        Variable predicateVar = new Variable();
        SchemaConcept schemaConcept = parent.tx().getSchemaConcept(label);
        ConceptId predicateId = schemaConcept != null ? schemaConcept.id() : null;
        //isa part
        Statement resVar = new Statement(varName).has(Pattern.label(label));
        return HasAtom.create(resVar, predicateVar, predicateId, parent);
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
            AttributeType attributeTypeConcept = executor
                    .getConcept(property.attributeType().var())
                    .asAttributeType();

            if (property.isKey()) {
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
            AttributeType<?> attributeType = executor
                    .getConcept(property.attributeType().var())
                    .asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (property.isKey()) {
                    type.unkey(attributeType);
                } else {
                    type.unhas(attributeType);
                }
            }
        }
    }
}
