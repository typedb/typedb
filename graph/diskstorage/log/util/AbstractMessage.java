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
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.log.Message;

import java.time.Instant;
import java.util.Objects;

/**
 * Abstract implementation of Message which exposes the timestamp, sender, and payload
 * of a message.
 * Particular Log implementations can extend this class.
 */
public abstract class AbstractMessage implements Message {

    private static final int MAX_PAYLOAD_STR_LENGTH = 400;

    private final StaticBuffer content;
    private final Instant timestamp;
    private final String senderId;

    protected AbstractMessage(StaticBuffer content, Instant timestamp, String senderId) {
        Preconditions.checkArgument(content != null && senderId != null);
        this.content = content;
        this.timestamp = timestamp;
        this.senderId = senderId;
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public StaticBuffer getContent() {
        return content;
    }

    @Override
    public String toString() {
        String payloadString = content.toString();
        if (payloadString.length() > MAX_PAYLOAD_STR_LENGTH) {
            payloadString = payloadString.substring(0, MAX_PAYLOAD_STR_LENGTH) + "...";
        }
        return "Message@" + timestamp + ":" + senderId + "=" + payloadString;
    }

    @Override
    public int hashCode() {
        return Objects.hash(content, timestamp, senderId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!getClass().isInstance(other)) {
            return false;
        }
        AbstractMessage msg = (AbstractMessage) other;
        return timestamp.equals(msg.timestamp) && senderId.equals(msg.senderId) && content.equals(msg.content);
    }
}
