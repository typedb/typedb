package grakn.core.server.session.cache;

import grakn.core.graql.reasoner.cache.MultilevelSemanticCache;
import grakn.core.server.session.TransactionOLTP;

public class CacheFactory {

    public CacheFactory() {

    }

    public RuleCache getRuleCache(TransactionOLTP tx) {
        return new RuleCache(tx);
    }

    public MultilevelSemanticCache getQueryCache() {
        return new MultilevelSemanticCache();
    }
}
