/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graph.core.schema;


public class JobStatus {

    public enum State { UNKNOWN, RUNNING, DONE, FAILED }

    private final State state;
    private final long numProcessed;

    public JobStatus(State state, long numProcessed) {
        this.state = state;
        this.numProcessed = numProcessed;
    }

    public State getState() {
        return state;
    }

    public boolean isDone() {
        return state== State.DONE || state== State.UNKNOWN;
    }

    public boolean hasFailed() {
        return state== State.FAILED;
    }

    public boolean isRunning() {
        return state== State.RUNNING;
    }

    public long getNumProcessed() {
        return numProcessed;
    }

}
