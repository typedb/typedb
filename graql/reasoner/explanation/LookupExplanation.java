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

package grakn.core.graql.reasoner.explanation;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;

import java.util.ArrayList;
import java.util.List;

/**
 * Explanation class for db lookup.
 */
public class LookupExplanation extends Explanation {

    private LookupExplanation(List<ConceptMap> answers){
        super(answers);
    }

    public LookupExplanation() {
        super(new ArrayList<>());
    }

    @Override
    public LookupExplanation childOf(ConceptMap ans) {
        return new LookupExplanation(ans.explanation().getAnswers());
    }

    @Override
    public boolean isLookupExplanation(){ return true;}
}
