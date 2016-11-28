/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.postprocessing;

import ai.grakn.engine.backgroundtasks.BackgroundTask;
import ai.grakn.engine.util.ConfigProperties;
import org.json.JSONObject;

import java.util.function.Consumer;

import static ai.grakn.engine.util.ConfigProperties.TIME_LAPSE;

public class PostProcessingTask implements BackgroundTask {
    private static final ConfigProperties properties = ConfigProperties.getInstance();
    private static final PostProcessing postProcessing = PostProcessing.getInstance();
    private static final Cache cache = Cache.getInstance();

    private static final long timeLapse = properties.getPropertyAsLong(TIME_LAPSE);

    /**
     * Run postprocessing only if enough time has passed since the last job was added
     * @param saveCheckpoint Consumer<String> which can be called at any time to save a state checkpoint that would allow
     * @param configuration
     */
    public void start(Consumer<String> saveCheckpoint, JSONObject configuration) {
        long lastJob = cache.getLastTimeJobAdded();
        long currentTime = System.currentTimeMillis();
        if((currentTime - lastJob) >= timeLapse)
            postProcessing.run();
    }

    public void stop() {
        postProcessing.stop();
    }

    public void pause() {
    }

    public void resume(Consumer<String> saveCheckpoint, String lastCheckpoint) {
    }
}