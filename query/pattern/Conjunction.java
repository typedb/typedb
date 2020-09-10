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

package grakn.core.query.pattern;

import grakn.core.query.pattern.variable.TypeVariable;
import grakn.core.query.pattern.variable.Variable;
import grakn.core.query.pattern.variable.VariableRegistry;
import graql.lang.pattern.variable.BoundVariable;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Conjunction<PATTERN extends Pattern> extends Pattern {

    private final Set<PATTERN> patterns;

    private Conjunction(Set<PATTERN> patterns) {
        this.patterns = patterns;
    }

    public static Conjunction<TypeVariable> fromTypes(final List<graql.lang.pattern.variable.TypeVariable> variables) {
        VariableRegistry register = new VariableRegistry();
        LinkedList<graql.lang.pattern.variable.TypeVariable> list = new LinkedList<>(variables);
        while (!list.isEmpty()) {
            graql.lang.pattern.variable.TypeVariable variable = list.removeFirst();
            assert variable.isLabelled();
            variable.constraints().forEach(c -> list.addAll(c.variables()));
            register.register(variable);
        }
        return new Conjunction<>(register.types());
    }

    public static Conjunction<Variable> fromThings(final List<graql.lang.pattern.variable.ThingVariable<?>> variables) {
        VariableRegistry register = new VariableRegistry();
        Set<Variable> output = new HashSet<>();
        LinkedList<BoundVariable> list = new LinkedList<>(variables);

        while (!list.isEmpty()) {
            BoundVariable graqlVar = list.removeFirst();
            graqlVar.constraints().forEach(c -> list.addAll(c.variables()));
            register.register(graqlVar);
        }

        output.addAll(register.types());
        output.addAll(register.things());
        return new Conjunction<>(output);
    }

    public Set<PATTERN> patterns() {
        return patterns;
    }
}
