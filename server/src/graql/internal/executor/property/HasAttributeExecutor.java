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
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.neq;
import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.rolePlayer;

public class HasAttributeExecutor implements PropertyExecutor.Insertable, PropertyExecutor.Matchable {

    private final Variable var;
    private final HasAttributeProperty property;

    public HasAttributeExecutor(Variable var, HasAttributeProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertHasAttribute());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Label type = property.type();
        Label has = Schema.ImplicitType.HAS.getLabel(type);
        Label key = Schema.ImplicitType.KEY.getLabel(type);

        Label hasOwnerRole = Schema.ImplicitType.HAS_OWNER.getLabel(type);
        Label keyOwnerRole = Schema.ImplicitType.KEY_OWNER.getLabel(type);
        Label hasValueRole = Schema.ImplicitType.HAS_VALUE.getLabel(type);
        Label keyValueRole = Schema.ImplicitType.KEY_VALUE.getLabel(type);

        Variable edge1 = Pattern.var();
        Variable edge2 = Pattern.var();

        return ImmutableSet.of(
                //owner rolePlayer edge
                rolePlayer(property, property.relationship().var(), edge1, var, null, ImmutableSet.of(hasOwnerRole, keyOwnerRole), ImmutableSet.of(has, key)),
                //value rolePlayer edge
                rolePlayer(property, property.relationship().var(), edge2, property.attribute().var(), null, ImmutableSet.of(hasValueRole, keyValueRole), ImmutableSet.of(has, key)),
                neq(property, edge1, edge2)
        );
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
            Set<Variable> produced = new HashSet<>();
            produced.add(var);
            produced.add(property.attribute().var());
            return Collections.unmodifiableSet(produced);
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(property.relationship().var());
        }

        @Override
        public void execute(WriteExecutor executor) {
            Attribute attributeConcept = executor.getConcept(property.attribute().var()).asAttribute();
            Thing thing = executor.getConcept(var).asThing();
            ConceptId relationshipId = thing.relhas(attributeConcept).id();
            executor.getBuilder(property.relationship().var()).id(relationshipId);
        }
    }
}
