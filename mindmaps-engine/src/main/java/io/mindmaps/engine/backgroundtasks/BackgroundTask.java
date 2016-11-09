/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.backgroundtasks;

import java.util.Map;

/**
 * Interface which all tasks that wish to be scheduled for later execution as background tasks must implement.
 */
public interface BackgroundTask {
    /**
     * Called to start execution of the task, may be called on a newly scheduled or previously stopped task.,
     */
    void start();

    /**
     * Called to stop execution of the task, may be called on a running or paused task.
     * Task should stop execution immediately and may be killed after this method exits.
     */
    void stop();

    /**
     * Called to suspend the execution of a currently running task. The object may be destroyed after this call.
     * @return Map<> that will be passed to resume() to resume the task.
     */
    Map<String, Object> pause();

    /**
     * Called to restore state necessary for execution to a previously suspended point by pause() however resume() itself
     * should not aim to resume execution as this will be accomplished by a subsequent call to start().
     * @param m Map<> as returned by pause()
     */
    void resume(Map<String, Object> m);

    /**
     * Called to restart a task regardless of where it is in its execution. The task should clean up any internal state
     * and perform any actions necessary in order to successfully complete its execution as if it was starting from a
     * clean state.
     */
    void restart();
}
