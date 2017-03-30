/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.ReasonerQuery;
import java.util.HashSet;

/**
 *
 * <p>
 * Explanation class for db lookup.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class LookupExplanation extends Explanation {


    public LookupExplanation(ReasonerQuery q){ super(q, new HashSet<>());}
    public LookupExplanation(LookupExplanation exp){
        super(exp.getQuery(), exp.getAnswers());
    }

    @Override
    public AnswerExplanation copy(){ return new LookupExplanation(this);}

    @Override
    public boolean isLookupExplanation(){ return true;}
}
