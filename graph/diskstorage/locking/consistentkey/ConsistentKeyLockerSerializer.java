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

package grakn.core.graph.diskstorage.locking.consistentkey;

import grakn.core.graph.diskstorage.ReadBuffer;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import grakn.core.graph.diskstorage.locking.consistentkey.TimestampRid;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.WriteBufferUtil;
import grakn.core.graph.diskstorage.util.WriteByteBuffer;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;

/**
 * Translate locking coordinates and metadata (data keys, data columns, data
 * values, timestamps, and rids) into keys, columns, and values compatible with
 * {@link ConsistentKeyLocker} and vice-versa.
 */
public class ConsistentKeyLockerSerializer {
     
    public StaticBuffer toLockKey(StaticBuffer key, StaticBuffer column) {
        WriteBuffer b = new WriteByteBuffer(key.length() + column.length() + 4);
        b.putInt(key.length());
        WriteBufferUtil.put(b,key);
        WriteBufferUtil.put(b,column);
        return b.getStaticBuffer();
    }
    
    public StaticBuffer toLockCol(Instant ts, StaticBuffer rid, TimestampProvider provider) {
        WriteBuffer b = new WriteByteBuffer(rid.length() + 8);
        b.putLong(provider.getTime(ts));
        WriteBufferUtil.put(b, rid);
        return b.getStaticBuffer();
    }
    
    public TimestampRid fromLockColumn(StaticBuffer lockKey, TimestampProvider provider) {
        ReadBuffer r = lockKey.asReadBuffer();
        int len = r.length();
        long tsNS = r.getLong();
        len -= 8;
        byte[] curRid = new byte[len];
        for (int i = 0; r.hasRemaining(); i++) {
            curRid[i] = r.getByte();
        }
        StaticBuffer rid = new StaticArrayBuffer(curRid);
        Instant time = provider.getTime(tsNS);
        return new TimestampRid(time, rid);
    }
}
