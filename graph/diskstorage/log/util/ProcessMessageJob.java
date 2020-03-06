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

package grakn.core.graph.diskstorage.log.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.log.Message;
import grakn.core.graph.diskstorage.log.MessageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for processing read messages with the registered message readers.
 * Simple implementation of a Runnable.
 */
public class ProcessMessageJob implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessMessageJob.class);

    private final Message message;
    private final MessageReader reader;

    public ProcessMessageJob(Message message, MessageReader reader) {
        Preconditions.checkArgument(message != null && reader != null);
        this.message = message;
        this.reader = reader;
    }

    @Override
    public void run() {
        try {
            LOG.debug("Passing {} to {}", message, reader);
            reader.read(message);
        } catch (Throwable e) {
            LOG.error("Encountered exception when processing message [" + message + "] by reader [" + reader + "]:", e);
        }
    }
}
