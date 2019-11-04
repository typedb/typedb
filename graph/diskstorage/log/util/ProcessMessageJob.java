// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.diskstorage.log.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.log.Message;
import grakn.core.graph.diskstorage.log.MessageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for processing read messages with the registered message readers.
 * Simple implementation of a {@link Runnable}.
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
