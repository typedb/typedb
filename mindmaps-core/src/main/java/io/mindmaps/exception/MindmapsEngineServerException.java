/*
 *  MindmapsDB - A Distributed Semantic Database
 *  Copyright (C) 2016  Mindmaps Research Ltd
 *
 *  MindmapsDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MindmapsDB is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.exception;

import org.slf4j.LoggerFactory;

/**
 * This Exception is thrown by Mindmaps Engine web server when operations accessible through APIs go wrong.
 */
public class MindmapsEngineServerException extends RuntimeException {

    int status;

    public MindmapsEngineServerException(int status, String message) {
        super(message);
        LoggerFactory.getLogger(MindmapsEngineServerException.class).error("New Mindmaps Engine Server exception {}", message);
        this.status = status;
    }

    public MindmapsEngineServerException(int status, Exception e) {
        super(e.toString());
        LoggerFactory.getLogger(MindmapsEngineServerException.class).error("New Mindmaps Engine Server exception", e);
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }
}
