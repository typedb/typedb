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
 *
 */

package ai.grakn.engine.tasks;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Scheduling information for tasks, contained within a {@link TaskState}.
 *
 * @author Felix Chapman
 */
public class TaskSchedule implements Serializable {

    private static final long serialVersionUID = 8220146809708041152L;

    /**
     * When this task should be executed.
     */
    private final Instant runAt;

    /**
     * If a task is marked as recurring, this represents the time delay between the next executing of this task.
     */
    private final @Nullable Duration interval;

    /**
     * Create a schedule to run a task now.
     */
    public static TaskSchedule now() {
        return at(Instant.now());
    }

    /**
     * Create a schedule to run a task at a specified instant.
     */
    public static TaskSchedule at(Instant instant) {
        return new TaskSchedule(instant, null);
    }

    /**
     * Create a schedule to run a task that recurs on an interval.
     */
    public static TaskSchedule recurring(Duration interval) {
        return recurring(Instant.now(), interval);
    }

    /**
     * Create a schedule to run a task that recurs on an interval, starting at the specified instance.
     */
    public static TaskSchedule recurring(Instant instant, Duration interval) {
        return new TaskSchedule(instant, requireNonNull(interval));
    }

    private TaskSchedule(Instant runAt, @Nullable Duration interval) {
        this.runAt = requireNonNull(runAt);
        this.interval = interval;
    }

    /**
     * Get the instant the task is specified to run at.
     */
    public Instant runAt() {
        return runAt;
    }

    /**
     * Get the interval that the task should recur, if it is a recurring task.
     */
    public Optional<Duration> interval() {
        return Optional.ofNullable(interval);
    }

    /**
     * Get whether the task is a recurring task.
     */
    public boolean isRecurring() {
        return interval != null;
    }

    /**
     * Returns a copy of this Schedule that will start a task after the given interval
     * @return a Schedule with the given amount added
     */
    public TaskSchedule incrementByInterval(){
        return new TaskSchedule(runAt.plus(interval), interval);
    }
}
