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

package grakn.core.server.util;

import com.google.common.util.concurrent.Striped;

import java.util.concurrent.locks.Lock;

/**
 * Simple locking mechanism that can be used in case of single server execution
 */
public class LockManager {

    private Striped<Lock> locks = Striped.lazyWeakLock(128);

    public Lock getLock(String lockToObtain) {
        return locks.get(lockToObtain);
    }
}
