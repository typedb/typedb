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

import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.executor.Writer;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HasAttributeTypeExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final HasAttributeTypeProperty property;

    public HasAttributeTypeExecutor(Variable var, HasAttributeTypeProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> defineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new DefineHasAttributeType()));
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> undefineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new UndefineHasAttributeType()));
    }

    private abstract class AbstractWriteExecutor {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.type().var());

            return Collections.unmodifiableSet(required);
        }

        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }
    }

    private class DefineHasAttributeType extends AbstractWriteExecutor implements PropertyExecutor.WriteExecutor {

        @Override
        public void execute(Writer writer) {
            Type entityTypeConcept = writer.getConcept(var).asType();
            AttributeType attributeTypeConcept = writer.getConcept(property.type().var()).asAttributeType();

            if (property.isRequired()) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.has(attributeTypeConcept);
            }
        }
    }

    private class UndefineHasAttributeType extends AbstractWriteExecutor implements PropertyExecutor.WriteExecutor {

        @Override
        public void execute(Writer writer) {
            Type type = writer.getConcept(var).asType();
            AttributeType<?> attributeType = writer.getConcept(property.type().var()).asAttributeType();

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
