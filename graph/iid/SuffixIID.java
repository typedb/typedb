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

package grakn.core.graph.iid;

import java.util.Arrays;

public class SuffixIID extends IID {

    private SuffixIID(byte[] bytes) {
        super(bytes);
    }

    public static SuffixIID of(byte[] bytes) {
        return new SuffixIID(bytes);
    }

    @Override
    public String toString() {
        if (readableString == null) readableString = "Suffix: " + Arrays.toString(bytes);
        return readableString;
    }
}
