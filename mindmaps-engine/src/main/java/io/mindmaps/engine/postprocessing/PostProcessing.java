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

package io.mindmaps.engine.postprocessing;

import io.mindmaps.engine.backgroundtasks.types.BackgroundTask;
import io.mindmaps.engine.backgroundtasks.TaskStatus;

public class PostProcessing extends BackgroundTask {
    public PostProcessing() {
        super(PostProcessing.class.getName());
    }

    public void start() {
        if(this.getStatus() == TaskStatus.SCHEDULED)
            this.setStatus(TaskStatus.RUNNING)
                .setStatuChangeMessage("Starting post processing")
                .setStatusChangedBy(PostProcessing.class.getName());

        if(this.getStatus() == TaskStatus.RUNNING)
            BackgroundTasks.getInstance().performPostprocessing();
    }

    public void stop() {
        this.setStatus(TaskStatus.STOPPED);
        this.setStatusChangedBy(Thread.currentThread().getStackTrace()[1].toString());
        this.setStatuChangeMessage("stop() called");
    }
}
