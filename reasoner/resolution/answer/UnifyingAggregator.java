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
import grakn.core.logic.Unifier;

public class UnifyingAggregator extends Aggregator {

    public static UnifyingAggregator of(ConceptMap conceptMap, Unifier unifier) {
        return new UnifyingAggregator(conceptMap, unifier);
    }

    UnifyingAggregator(ConceptMap conceptMap, Unifier unifier) {
        super(conceptMap);

    }

    @Override
    public boolean equals(Object o) {
        return false; // TODO implement
    }

    @Override
    ConceptMap transform(ConceptMap original) {
        return null;
    }

    @Override
    ConceptMap unTransform(ConceptMap conceptMap) {
        return null;
    }
}
