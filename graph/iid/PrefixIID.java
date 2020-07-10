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

package grakn.core.graph.iid;

import grakn.core.graph.util.Schema;

public class PrefixIID extends IID {

    public static final int LENGTH = 1;

    private PrefixIID(byte[] bytes) {
        super(bytes);
        assert bytes.length == LENGTH;
    }

    public static PrefixIID of(Schema.Prefix prefix) {
        return new PrefixIID(prefix.bytes());
    }

    @Override
    public String toString() {
        if (readableString == null) readableString = "[" + Schema.Prefix.of(bytes[0]).toString() + "]";
        return readableString;
    }
}
