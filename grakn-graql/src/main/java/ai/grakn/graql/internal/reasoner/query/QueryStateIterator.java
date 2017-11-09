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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.internal.reasoner.state.QueryStateBase;
import java.util.Iterator;

/**
 *
 * <p>
 * Container class for storing a db iterator with its cache unifier as well as the sub goal iterator.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryStateIterator  {

    private final Iterator<Answer> dbIterator;
    private final MultiUnifier cacheUnifier;

    private final Iterator<QueryStateBase> subGoalIterator;

    QueryStateIterator(Iterator<Answer> dbIterator, MultiUnifier cacheUnifier, Iterator<QueryStateBase> subGoalIterator){
        this.dbIterator = dbIterator;
        this.cacheUnifier = cacheUnifier;
        this.subGoalIterator = subGoalIterator;
    }

    public Iterator<Answer> dbIterator(){ return dbIterator;}
    public MultiUnifier cacheUnifier(){ return cacheUnifier;}
    public Iterator<QueryStateBase> subGoalIterator(){ return subGoalIterator;}
}
