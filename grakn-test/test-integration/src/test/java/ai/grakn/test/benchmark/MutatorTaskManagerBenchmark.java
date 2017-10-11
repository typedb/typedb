package ai.grakn.test.benchmark;

import ai.grakn.test.EngineContext;


public class MutatorTaskManagerBenchmark extends MutatorTaskBenchmark {
    protected EngineContext makeEngine() {
        return EngineContext.createWithInMemoryRedis();
    }
}
