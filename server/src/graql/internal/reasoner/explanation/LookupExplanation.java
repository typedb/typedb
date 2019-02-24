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

package grakn.core.graql.internal.reasoner.explanation;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Explanation;
import graql.lang.pattern.Pattern;

import java.util.List;

/**
 * Explanation class for db lookup.
 */
public class LookupExplanation extends Explanation {

    public LookupExplanation(Pattern pattern){
        super(pattern);
    }

    private LookupExplanation(Pattern pattern, List<ConceptMap> answers){
        super(pattern, answers);
    }

    @Override
    public LookupExplanation setPattern(Pattern pattern){
        return new LookupExplanation(pattern);
    }

    @Override
    public LookupExplanation childOf(ConceptMap ans) {
        return new LookupExplanation(getPattern(), ans.explanation().getAnswers());
    }

    @Override
    public boolean isLookupExplanation(){ return true;}
}
