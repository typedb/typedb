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
 *
 */

package grakn.core.common.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CommonExecutorService {

    private static ExecutorService service;

    public static ExecutorService init(final int threadCount) {
        service = Executors.newWorkStealingPool(threadCount);
        return service;
    }

    public static ExecutorService get() {
        assert service != null;
        return service;
    }
}
