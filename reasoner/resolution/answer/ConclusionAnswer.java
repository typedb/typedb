/*
 * Copyright (C) 2021 Grakn Labs
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
import grakn.core.logic.resolvable.Unifier;
import grakn.core.pattern.Conjunction;

public class ConclusionAnswer {

    private final Conjunction conclusionPattern;
    private final ConceptMap conceptMap;
    private final Unifier unifier;

    public ConclusionAnswer(Conjunction conclusionPattern, ConceptMap conceptMap, Unifier unifier) {
        this.conclusionPattern = conclusionPattern;
        this.conceptMap = conceptMap;
        this.unifier = unifier;
    }
}
