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

package grakn.core.graql.reasoner;

import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.cache.QueryCache;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;

/**
 * Container class for reasoning-related entities - factories and caches.
 */
public class ReasoningContext {

    private final ReasonerQueryFactory queryFactory;
    private final ConceptManager conceptManager;
    private final RuleCache ruleCache;
    private final QueryCache queryCache;
    private final KeyspaceStatistics keyspaceStatistics;

    public ReasoningContext(ReasonerQueryFactory queryFactory,
                     ConceptManager conceptManager,
                     QueryCache queryCache,
                     RuleCache ruleCache,
                     KeyspaceStatistics keyspaceStatistics){
        this.queryFactory = queryFactory;
        this.conceptManager = conceptManager;
        this.queryCache = queryCache;
        this.ruleCache = ruleCache;
        this.keyspaceStatistics = keyspaceStatistics;
    }

    public ReasonerQueryFactory queryFactory(){return queryFactory;}
    public ConceptManager conceptManager(){return conceptManager;}
    public QueryCache queryCache(){return queryCache;}
    public RuleCache ruleCache(){return ruleCache;}
    public KeyspaceStatistics keyspaceStatistics(){return keyspaceStatistics;}
}
