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

package grakn.core.reasoner.resolution.answer;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.variable.Variable;

import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MappingAggregator extends Aggregator {

    private Map<Variable, Variable> mapping;

    public static MappingAggregator of(ConceptMap conceptMap, Map<Variable, Variable> variableMap) {
        return new MappingAggregator(conceptMap, variableMap);
    }

    public static Map<Variable, Variable> identityMapping(ConjunctionConcludable<?, ?> concludable) {
        // TODO Should this helper live in AlphaEquivalence?
        return new HashSet<>(concludable.constraint().variables()).stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    MappingAggregator(ConceptMap conceptMap, Map<Variable, Variable> mapping) {
        super(conceptMap);
        this.mapping = mapping;
    }

    @Override
    ConceptMap transform(ConceptMap original) {
        return null; // TODO
    }

    @Override
    ConceptMap unTransform(ConceptMap conceptMap) {
        return null; // TODO
    }

    @Override
    public boolean equals(Object o) {
        return false; // TODO implement
    }
}
