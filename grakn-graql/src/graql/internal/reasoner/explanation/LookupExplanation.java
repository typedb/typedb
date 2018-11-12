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
import grakn.core.graql.admin.Explanation;
import grakn.core.graql.admin.ReasonerQuery;
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

    public LookupExplanation(ReasonerQuery q){ super(q);}
    private LookupExplanation(ReasonerQuery q, List<ConceptMap> answers){
        super(q, answers);
    }

    @Override
    public Explanation setQuery(ReasonerQuery q){
        return new LookupExplanation(q);
    }

    @Override
    public Explanation childOf(ConceptMap ans) {
        return new LookupExplanation(getQuery(), ans.explanation().getAnswers());
    }

    @Override
    public boolean isLookupExplanation(){ return true;}
}
