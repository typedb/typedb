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


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTask implements BackgroundTask {
    private static AtomicInteger runCount = new AtomicInteger(0);

    public void start() {
        runCount.incrementAndGet();
    }

    public void stop() {}

    public Map<String, Object> pause() {
        return new HashMap<>();
    }

    public void resume(Map<String, Object> m) {}

    public void restart() {
        runCount.set(0);
    }

    public int getRunCount() {
        return runCount.get();
    }
}
