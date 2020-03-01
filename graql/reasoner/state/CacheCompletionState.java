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

package grakn.core.graql.reasoner.state;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.CacheCasting;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.kb.graql.reasoner.cache.QueryCache;

/**
 *
 * State used to acknowledge db completion of a query in the cache - all db answers to the query are cached.
 *
 */
public class CacheCompletionState extends ResolutionState {

    private QueryCache queryCache;
    final private ReasonerAtomicQuery query;

    public CacheCompletionState(QueryCache queryCache, ReasonerAtomicQuery query, ConceptMap sub, AnswerPropagatorState parent) {
        super(sub, parent);
        this.queryCache = queryCache;
        this.query = query;
    }

    @Override
    public ResolutionState generateChildState() {
        CacheCasting.queryCacheCast(queryCache).ackDBCompleteness(query);
        return null;
    }

}
