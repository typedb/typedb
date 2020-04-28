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
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.graql.exception.GraqlQueryException;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.neq;
import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.rolePlayer;

public class HasAttributeExecutor  implements PropertyExecutor.Insertable, PropertyExecutor.Deletable {

    private final Variable var;
    private final HasAttributeProperty property;
    private final Label type;

    HasAttributeExecutor(Variable var, HasAttributeProperty property) {
        this.var = var;
        this.property = property;
        this.type = Label.of(property.type());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Label has = Schema.ImplicitType.HAS.getLabel(type);
        Label key = Schema.ImplicitType.KEY.getLabel(type);

        Label hasOwnerRole = Schema.ImplicitType.HAS_OWNER.getLabel(type);
        Label keyOwnerRole = Schema.ImplicitType.KEY_OWNER.getLabel(type);
        Label hasValueRole = Schema.ImplicitType.HAS_VALUE.getLabel(type);
        Label keyValueRole = Schema.ImplicitType.KEY_VALUE.getLabel(type);

        Variable edge1 = new Variable();
        Variable edge2 = new Variable();

        return ImmutableSet.of(
                //owner rolePlayer edge
                rolePlayer(property, property.relation().var(), edge1, var, null,
                           ImmutableSet.of(hasOwnerRole, keyOwnerRole), ImmutableSet.of(has, key)),
                //value rolePlayer edge
                rolePlayer(property, property.relation().var(), edge2, property.attribute().var(), null,
                           ImmutableSet.of(hasValueRole, keyValueRole), ImmutableSet.of(has, key)),
                neq(property, edge1, edge2)
        );
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertHasAttribute());
    }

    @Override
    public Set<Writer> deleteExecutors() {
        return ImmutableSet.of(new DeleteHasAttribute());
    }

    private class InsertHasAttribute implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.attribute().var());
            return Collections.unmodifiableSet(required);
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(property.relation().var());
        }

        @Override
        public void execute(WriteExecutor executor) {
            Attribute attributeConcept = executor.getConcept(property.attribute().var()).asAttribute();
            Thing thing = executor.getConcept(var).asThing();
            ConceptId relationId = thing.relhas(attributeConcept).id();
            executor.getBuilder(property.relation().var()).id(relationId);
        }
    }

    private class DeleteHasAttribute implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.attribute().var());
            return Collections.unmodifiableSet(required);
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }

        @Override
        public TiebreakDeletionOrdering ordering(WriteExecutor executor) {
            return TiebreakDeletionOrdering.EDGE;
        }

        @Override
        public void execute(WriteExecutor executor) {
            Variable attributeVar = property.attribute().var();
            Concept concept = executor.getConcept(attributeVar);
            if (!concept.isAttribute()) {
                throw GraqlSemanticException.cannotDeleteOwnershipOfNonAttributes(attributeVar, concept);
            }
            Attribute<?> attribute = concept.asAttribute();

            // deleting the ownership of an instance of a type 'type' should throw if matched concept is not that type
            if (attribute.type().sups().noneMatch(sub -> sub.label().equals(type))) {
                throw GraqlSemanticException.cannotDeleteOwnershipTypeNotSatisfied(attributeVar, attribute, type);
            }
            Thing owner = executor.getConcept(var).asThing();
            owner.unhas(attribute);
        }
    }
}
