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

import com.google.common.collect.ImmutableSet;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutorFactory;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.Graql;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.NeqProperty;
import graql.lang.property.PlaysProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.core.Schema.ImplicitType.KEY;
import static grakn.core.core.Schema.ImplicitType.KEY_OWNER;
import static grakn.core.core.Schema.ImplicitType.KEY_VALUE;
import static graql.lang.Graql.var;

public class HasAttributeTypeExecutor  implements PropertyExecutor.Definable {

    private final Variable var;
    private final HasAttributeTypeProperty property;

    private final Statement attributeType;
    private final Statement ownerRole;
    private final Statement valueRole;
    private final Statement relationOwner;
    private final Statement relationValue;

    HasAttributeTypeExecutor(Variable var, HasAttributeTypeProperty property, PropertyExecutorFactory propertyExecutorFactory) {
        this.var = var;
        this.property = property;
        this.attributeType = property.attributeType();

        // TODO: this may the cause of issue #4664
        String type = attributeType.getType().orElseThrow(
                () -> GraqlSemanticException.noLabelSpecifiedForHas(attributeType.var())
        );

        Statement role = Graql.type(Graql.Token.Type.ROLE);

        // TODO: To fix issue #4664 (querying schema with variable attribute type), it's not enough to just remove the
        //       exception handling above. We need to be able to restrict the direcitonality of the following ownerRole
        //       and valueRole. For example, for keys, we restrict the role names to start with `key-`. However, we
        //       cannot do this because the name of the variable attribute type is unknown. This means that we need to
        //       change the data structure so that we have meta-super-roles for attribute-owners and attribute-values
        Statement ownerRole = var().sub(role);
        Statement valueRole = var().sub(role);
        Statement relationType = var().sub(Graql.type(Graql.Token.Type.RELATION));

        // If a key, limit only to the implicit key type
        if (property.isKey()) {
            ownerRole = ownerRole.type(KEY_OWNER.getLabel(type).getValue());
            valueRole = valueRole.type(KEY_VALUE.getLabel(type).getValue());
            relationType = relationType.type(KEY.getLabel(type).getValue());
        }

        Statement relationOwner = relationType.relates(ownerRole);
        Statement relationValue = relationType.relates(valueRole);

        this.ownerRole = ownerRole;
        this.valueRole = valueRole;
        if (relationOwner == null) {
            throw new NullPointerException("Null relationOwner");
        }
        this.relationOwner = relationOwner;
        if (relationValue == null) {
            throw new NullPointerException("Null relationValue");
        }
        this.relationValue = relationValue;

    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Set<EquivalentFragmentSet> fragments = new HashSet<>();

        PlaysProperty playsOwnerProperty = new PlaysProperty(ownerRole, property.isKey());
        PlaysExecutor playsOwnerExecutor = new PlaysExecutor(var, playsOwnerProperty);

        //TODO: Get this to use real constraints no just the required flag
        PlaysProperty playsValueProperty = new PlaysProperty(valueRole, false);
        PlaysExecutor playsValueExecutor = new PlaysExecutor(attributeType.var(), playsValueProperty);

        NeqProperty neqProperty = new NeqProperty(ownerRole);
        NeqExecutor neqExecutor = new NeqExecutor(valueRole.var(), neqProperty);

        // Add fragments for HasAttributeType property
        fragments.addAll(playsOwnerExecutor.matchFragments());
        fragments.addAll(playsValueExecutor.matchFragments());
        fragments.addAll(neqExecutor.matchFragments());

        // Add fragments for the implicit relation property
        // These implicit statements make sure that statement variable (owner) and the attribute type form a
        // connected (non-disjoint) set of fragments for match execution

        PropertyExecutorFactory propertyExecutorFactory = new PropertyExecutorFactoryImpl();
        Stream.of(ownerRole, valueRole, relationOwner, relationValue)
                .forEach(statement -> statement.properties()
                        .forEach(p -> fragments.addAll(propertyExecutorFactory.create(statement.var(), p).matchFragments())));

        return ImmutableSet.copyOf(fragments);
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineHasAttributeType());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineHasAttributeType());
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
