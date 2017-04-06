package ai.grakn.engine.postprocessing;

import ai.grakn.engine.tasks.BackgroundTask;
import ai.grakn.engine.tasks.TaskCheckpoint;
import mjson.Json;

import java.util.function.Consumer;

public class UpdatingInstanceCountTask implements BackgroundTask {
    @Override
    public boolean start(Consumer<TaskCheckpoint> saveCheckpoint, Json configuration) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean resume(Consumer<TaskCheckpoint> saveCheckpoint, TaskCheckpoint lastCheckpoint) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
