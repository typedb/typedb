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

package ai.grakn.engine.backgroundtasks;

import org.json.JSONObject;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class LongRunningTask implements BackgroundTask {
    private AtomicBoolean isRunning = new AtomicBoolean(true);

    public void start(Consumer<String> saveCheckpoint, JSONObject config) {
        long initial = new Date().getTime();

        while (isRunning.get() && ((new Date().getTime()) - initial < 50000)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                break;
            }
        }
    }

    public void stop() {
        isRunning.set(false);
    }

    public void pause() {
    }

    public void resume(Consumer<String> c, String s) {
    }
}
