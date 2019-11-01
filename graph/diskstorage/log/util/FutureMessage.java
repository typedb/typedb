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
import com.google.common.util.concurrent.AbstractFuture;
import grakn.core.graph.diskstorage.log.Message;

/**
 * Implementation of a {@link java.util.concurrent.Future} for {@link Message}s that
 * are being added to the {@link org.janusgraph.diskstorage.log.Log} via {@link org.janusgraph.diskstorage.log.Log#add(org.janusgraph.diskstorage.StaticBuffer)}.
 *
 * This class can be used by {@link org.janusgraph.diskstorage.log.Log} implementations to wrap messages.
 *
 */
public class FutureMessage<M extends Message> extends AbstractFuture<Message> {

    private final M message;

    public FutureMessage(M message) {
        Preconditions.checkNotNull(message);
        this.message = message;
    }

    /**
     * Returns the actual message that was added to the LOG
     * @return
     */
    public M getMessage() {
        return message;
    }

    /**
     * This method should be called by {@link org.janusgraph.diskstorage.log.Log} implementations when the message was successfully
     * added to the LOG.
     */
    public void delivered() {
        super.set(message);
    }

    /**
     * This method should be called by {@link org.janusgraph.diskstorage.log.Log} implementations when the message could not be added to the LOG
     * with the respective exception object.
     * @param exception
     */
    public void failed(Throwable exception) {
        super.setException(exception);
    }

    @Override
    public String toString() {
        return "FutureMessage[" + message + "]";
    }
}
