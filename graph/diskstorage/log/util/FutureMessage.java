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

package grakn.core.graph.diskstorage.log.util;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.log.Log;
import grakn.core.graph.diskstorage.log.Message;

/**
 * Implementation of a {@link java.util.concurrent.Future} for {@link Message}s that
 * are being added to the {@link Log} via {@link Log#add(StaticBuffer)}.
 * <p>
 * This class can be used by {@link Log} implementations to wrap messages.
 */
public class FutureMessage<M extends Message> extends AbstractFuture<Message> {

    private final M message;

    public FutureMessage(M message) {
        Preconditions.checkNotNull(message);
        this.message = message;
    }

    /**
     * Returns the actual message that was added to the LOG
     */
    public M getMessage() {
        return message;
    }

    /**
     * This method should be called by {@link Log} implementations when the message was successfully
     * added to the LOG.
     */
    public void delivered() {
        super.set(message);
    }

    /**
     * This method should be called by {@link Log} implementations when the message could not be added to the LOG
     * with the respective exception object.
     */
    public void failed(Throwable exception) {
        super.setException(exception);
    }

    @Override
    public String toString() {
        return "FutureMessage[" + message + "]";
    }
}
