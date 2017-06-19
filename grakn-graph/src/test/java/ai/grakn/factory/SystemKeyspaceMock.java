package ai.grakn.factory;

import java.util.concurrent.CountDownLatch;

/**
 * Mock the system keyspace class for a test in which we need to dereference the factory variable
 * @author alexandraorth
 */
class SystemKeyspaceMock extends SystemKeyspace {

    private SystemKeyspaceMock() {
        super();
    }

    static void dereference(){
        factoryInstantiated = new CountDownLatch(1);
        factoryBeingInstantiated.set(false);
        factory = null;
    }
}