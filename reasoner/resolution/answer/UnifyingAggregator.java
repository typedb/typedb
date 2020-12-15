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
import graql.lang.pattern.variable.Reference;

import java.util.Map;
import java.util.Set;

public class UnifyingAggregator extends Aggregator {

    UnifyingAggregator(ConceptMap conceptMap, Map<Reference.Name, Set<Reference.Name>> unifier) {
        super(conceptMap, transform(conceptMap, unifier));
    }

    public static UnifyingAggregator of(ConceptMap conceptMap, Map<Reference.Name, Set<Reference.Name>> unifier) {
        return new UnifyingAggregator(conceptMap, unifier);
    }

    @Override
    public boolean equals(Object o) {
        return false; // TODO implement
    }

    @Override
    ConceptMap unTransform(ConceptMap conceptMap) {
        return null;
    }

    private static ConceptMap transform(ConceptMap original, Map<Reference.Name, Set<Reference.Name>> unifier) {
        return null;
    }
}
