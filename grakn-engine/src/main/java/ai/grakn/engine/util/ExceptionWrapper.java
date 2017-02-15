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

package ai.grakn.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Utility class to execute a Runnable and log any exceptions thrown without propagating them further.
 * @author Denis Igorevich Lobanov
 */
public class ExceptionWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ExceptionWrapper.class);

    public static void noThrow(RunnableWithExceptions fn, String errorMessage) {
        try {
            fn.run();
        }
        catch (Throwable t) {
            LOG.error(errorMessage + "\nThe exception was: " + getFullStackTrace(t));
        }
    }

    /**
     * Function interface that throws exception for use in the noThrow function
     * @param <E>
     */
    @FunctionalInterface
    public interface RunnableWithExceptions<E extends Exception> {
        void run() throws E;
    }
}
