/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.common.collection;

import java.util.Iterator;
import java.util.stream.Stream;

public class Streams {

    public static int compareSize(Stream<?> stream, int size) {
        long count = 0L;
        final Iterator<?> iterator = stream.iterator();

        while (iterator.hasNext()) {
            iterator.next();
            count++;
            if (count > size) return 1;
        }

        return count == size ? 0 : -1;
    }
}
