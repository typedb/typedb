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

package ai.grakn.graql.internal.reasoner.state;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import java.util.Set;

/**
 *
 * <p>
 * //TODO
 * </p>
 *
 * @author Kasper Piskorski
 */
class AnswerStateFactory {

    static ResolutionState create(Answer sub, Set<Unifier> mu, QueryState parent){
        if(sub.isEmpty()) return null;
        return mu.size() > 1?
                new MultiAnswerState(sub, mu, parent) :
                new AnswerState(sub, mu.iterator().next(), parent);
    }
}
