/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.explanation;

import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.Explanation;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Explanation class for a join explanation - resulting from merging atoms in a conjunction.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class JoinExplanation extends QueryExplanation {

    public JoinExplanation(List<ConceptMap> answers){ super(answers);}
    public JoinExplanation(ReasonerQueryImpl q, ConceptMap mergedAnswer){
        super(q, q.selectAtoms().stream()
                .map(at -> at.inferTypes(mergedAnswer.project(at.getVarNames())))
                .map(ReasonerQueries::atomic)
                .map(aq -> mergedAnswer.project(aq.getVarNames()).explain(new LookupExplanation(aq)))
                .collect(Collectors.toList())
        );
    }

    @Override
    public Explanation childOf(ConceptMap ans) {
        return new JoinExplanation(ReasonerUtils.listUnion(this.getAnswers(), ans.explanation().getAnswers()));
    }

    @Override
    public boolean isJoinExplanation(){ return true;}
}
