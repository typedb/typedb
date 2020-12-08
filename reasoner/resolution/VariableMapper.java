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
 */

package grakn.core.reasoner.resolution;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.variable.Variable;

import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VariableMapper implements ConceptMapTransformer{

    private Map<Variable, Variable> map;

    public static VariableMapper identity(ConjunctionConcludable<?, ?> concludable) {
        return fromVariableMapping(new HashSet<>(concludable.constraint().variables()).stream().collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    public static VariableMapper fromVariableMapping(Map<Variable, Variable> map) {
        return new VariableMapper(map); // TODO Create a unifier from a 1:1 variable mapping from an alpha equivalence. Perhaps unnecessary if Unifier and the Variable Mapping implement the same interface
    }

    VariableMapper(Map<Variable, Variable> map) {
        this.map = map;
    }

    public ConceptMap transform(ConceptMap conceptMap) {
        return null;
    }

    public ConceptMap unTransform(ConceptMap conceptMap) {
        return null;
    }
}
