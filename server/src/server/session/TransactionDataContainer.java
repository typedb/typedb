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
