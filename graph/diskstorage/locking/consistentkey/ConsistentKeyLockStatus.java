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


import grakn.core.graph.diskstorage.locking.LockStatus;

import java.time.Instant;

/**
 * The timestamps of a lock held by a {@link ConsistentKeyLocker}
 * and whether the held lock has or has not been checked.
 */
public class ConsistentKeyLockStatus implements LockStatus {

    private final Instant write;
    private final Instant expire;
    private boolean checked;

    public ConsistentKeyLockStatus(Instant written, Instant expire) {
        this.write = written;
        this.expire = expire;
        this.checked = false;
    }

    @Override
    public Instant getExpirationTimestamp() {
        return expire;
    }


    public Instant getWriteTimestamp() {
        return write;
    }

    public boolean isChecked() {
        return checked;
    }

    public void setChecked() {
        this.checked = true;
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + (checked ? 1231 : 1237);
        result = prime * result + ((expire == null) ? 0 : expire.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ConsistentKeyLockStatus other = (ConsistentKeyLockStatus) obj;
        if (checked != other.checked) {
            return false;
        }
        if (expire == null) {
            return other.expire == null;
        } else {
            return expire.equals(other.expire);
        }
    }
}
