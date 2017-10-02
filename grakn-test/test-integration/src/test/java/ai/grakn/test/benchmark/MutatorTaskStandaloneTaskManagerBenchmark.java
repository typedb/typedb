package ai.grakn.test.benchmark;

import ai.grakn.test.EngineContext;


public class MutatorTaskStandaloneTaskManagerBenchmark extends MutatorTaskBenchmark {
    protected EngineContext makeEngine() {
        return EngineContext.inMemoryServer();
    }
}
