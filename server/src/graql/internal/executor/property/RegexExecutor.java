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
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.Set;

public class RegexExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final RegexProperty property;

    public RegexExecutor(Variable var, RegexProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new DefineRegex()));
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new UndefineRegex()));
    }

    private abstract class RegexWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            return Collections.unmodifiableSet(Collections.singleton(var));
        }

        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }
    }

    private class DefineRegex extends RegexWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            executor.getConcept(var).asAttributeType().regex(property.regex());
        }
    }

    private class UndefineRegex extends RegexWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            AttributeType<Object> attributeType = executor.getConcept(var).asAttributeType();
            if (!attributeType.isDeleted() && property.regex().equals(attributeType.regex())) {
                attributeType.regex(null);
            }
        }
    }
}
