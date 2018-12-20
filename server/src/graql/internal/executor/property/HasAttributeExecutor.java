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

import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.internal.executor.Writer;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HasAttributeExecutor implements PropertyExecutor.Insertable {

    private final Variable var;
    private final HasAttributeProperty property;

    public HasAttributeExecutor(Variable var, HasAttributeProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> insertExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new InsertHasAttribute()));
    }

    private class InsertHasAttribute implements PropertyExecutor.WriteExecutor {

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
            return Collections.unmodifiableSet(Collections.singleton(property.relationship().var()));
        }

        @Override
        public void execute(Writer writer) {
            Attribute attributeConcept = writer.getConcept(property.attribute().var()).asAttribute();
            Thing thing = writer.getConcept(var).asThing();
            ConceptId relationshipId = thing.relhas(attributeConcept).id();
            writer.getBuilder(property.relationship().var()).id(relationshipId);
        }
    }
}
