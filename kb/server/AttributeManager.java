package grakn.core.kb.server;

import com.google.common.cache.Cache;
import grakn.core.kb.concept.api.ConceptId;

public interface AttributeManager {

    Cache<String, ConceptId> attributesCache();

    void ackAttributeInsert(String index, String txId);
    void ackAttributeDelete(String index);
    void ackCommit(String txId);

    boolean needsLock(String txId);

    void printEphemeralCache();

}
