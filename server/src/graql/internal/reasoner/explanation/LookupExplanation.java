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

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Explanation;
import java.util.List;

/**
 *
 * <p>
 * Explanation class for db lookup.
 * </p>
 *
 *
 */
public class LookupExplanation extends QueryExplanation {

    public LookupExplanation(String queryPattern){ super(queryPattern);}
    private LookupExplanation(String queryPattern, List<ConceptMap> answers){
        super(queryPattern, answers);
    }

    @Override
    public Explanation setQueryPattern(String queryPattern){
        return new LookupExplanation(queryPattern);
    }

    @Override
    public Explanation childOf(ConceptMap ans) {
        return new LookupExplanation(getQueryPattern(), ans.explanation().getAnswers());
    }

    @Override
    public boolean isLookupExplanation(){ return true;}
}
