/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.server.session;

import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.server.session.cache.RuleCache;
import grakn.core.server.session.cache.TransactionCache;
import grakn.core.server.statistics.UncomittedStatisticsDelta;

public class TransactionDataContainer {

    private TransactionCache transactionCache;
    private MultilevelSemanticCache queryCache;
    private RuleCache ruleCache;
    private UncomittedStatisticsDelta statistics;

    public void setTransactionCache(TransactionCache transactionCache) {
        this.transactionCache = transactionCache;
    }

    public void  setQueryCache(MultilevelSemanticCache queryCache) {
        this.queryCache = queryCache;
    }

    public void setRuleCache(RuleCache ruleCache) {
        this.ruleCache = ruleCache;
    }

    public void setStatisticsDelta(UncomittedStatisticsDelta statistics) {
        this.statistics = statistics;
    }

    public TransactionCache transactionCache() {
        return transactionCache;
    }

    public MultilevelSemanticCache queryCache() {
        return queryCache;
    }

    public RuleCache ruleCache() {
        return ruleCache;
    }

    public UncomittedStatisticsDelta statistics() {
        return statistics;
    }
}
