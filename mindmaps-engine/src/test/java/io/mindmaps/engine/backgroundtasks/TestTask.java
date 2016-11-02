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

import io.mindmaps.engine.backgroundtasks.types.BackgroundTask;
import io.mindmaps.engine.backgroundtasks.types.TaskStatus;

public class TestTask extends BackgroundTask {
    private int runCount;

    TestTask() {
        super(TestTask.class.getName());
        runCount = 0;
    }

    TestTask(String name) {
        super(name);
        runCount = 0;
    }

    public void start() {
        if(this.getStatus() == TaskStatus.SCHEDULED)
            this.setStatus(TaskStatus.RUNNING)
                .setStatusChangedBy(TestTask.class.getName());

        if(this.getStatus() == TaskStatus.COMPLETED)
            this.setStatus(TaskStatus.RUNNING)
                .setStatuChangeMessage("starting another run")
                .setStatusChangedBy(TestTask.class.getName());

        if(this.getStatus() == TaskStatus.RUNNING)
            runCount ++;

        setStatus(TaskStatus.COMPLETED).setStatusChangedBy(TestTask.class.getName());
    }

    public void stop() {
        setStatus(TaskStatus.STOPPED);
    }

    public int getRunCount() {
        return runCount;
    }
}
