/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.diskstorage.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

public class UncaughtExceptionLogger implements UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(UncaughtExceptionHandler.class);

    /*
     * I don't like duplicating a subset of org.slf4j.Level, but the slf4j API
     * as of 1.7.5 provides no general Logger.LOG(Level, String, Object...)
     * method. I can't seem to meta-program around this.
     */
    public enum UELevel implements UELogLevel {
        TRACE {
            public void dispatch(String message, Throwable t) {
                LOG.trace(message, t);
            }
        },
        DEBUG {
            public void dispatch(String message, Throwable t) {
                LOG.debug(message, t);
            }
        },
        INFO {
            public void dispatch(String message, Throwable t) {
                LOG.info(message, t);
            }
        },
        WARN {
            public void dispatch(String message, Throwable t) {
                LOG.warn(message, t);
            }
        },
        ERROR {
            public void dispatch(String message, Throwable t) {
                LOG.error(message, t);
            }
        }
    }

    private final UELevel level;

    public UncaughtExceptionLogger(UELevel level) {
        this.level = level;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        level.dispatch(String.format("Uncaught exception in thread " + t), e);
    }
}

interface UELogLevel {
    void dispatch(String message, Throwable t);
}
