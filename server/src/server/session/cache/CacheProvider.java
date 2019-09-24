package grakn.core.server.session.cache;

import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;

public class CacheProvider {

    private final RuleCache ruleCache;
    private final MultilevelSemanticCache queryCache;
    private final TransactionCache transactionCache;

    public CacheProvider(KeyspaceSchemaCache keyspaceSchemaCache) {
        this.ruleCache = new RuleCache();
        this.queryCache = new MultilevelSemanticCache();
        this.transactionCache = new TransactionCache(keyspaceSchemaCache);
    }

    public RuleCache getRuleCache() {
        return ruleCache;
    }

    public MultilevelSemanticCache getQueryCache() {
        return queryCache;
    }

    public TransactionCache getTransactionCache() {
        return transactionCache;
    }
}
