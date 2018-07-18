package ai.grakn.engine;

import org.junit.Test;

public class UniquenessProcessorTest {
    // Unique Attribute
    // - deduplicate
    // - optimise with janus unique index
    // - reduce by propertyUnique (deprecate)

    @Test
    public void attributeDeduplicationXXX() {
        // turn off janus index and propertyUnique
        // insert 2 "John"
        // wait
        // assert that there is only one real "John" and the duplicate is deleted
        // assert that every entity connected to the duplicate "John" is connected to the real "John"
    }
}
