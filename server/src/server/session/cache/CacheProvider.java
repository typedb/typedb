package grakn.core.server.session.cache;

import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;

public class CacheProvider {

    private KeyspaceSchemaCache keyspaceSchemaCache;

    public CacheProvider(KeyspaceSchemaCache keyspaceSchemaCache) {
        this.keyspaceSchemaCache = keyspaceSchemaCache;
    }

    public RuleCache getRuleCache() {
        return new RuleCache();
    }

    public MultilevelSemanticCache getQueryCache() {
        return new MultilevelSemanticCache();
    }

    public TransactionCache getTransactionCache() {
        return new TransactionCache(keyspaceSchemaCache);
    }
}
