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

package grakn.core.graph.util;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;

import java.util.function.BiFunction;

public interface Storage {

    boolean isOpen();

    KeyGenerator.Schema schemaKeyGenerator();

    KeyGenerator.Data dataKeyGenerator();

    byte[] get(byte[] key);

    byte[] getLastKey(byte[] prefix);

    void delete(byte[] key);

    void put(byte[] key);

    void put(byte[] key, byte[] value);

    void putUntracked(byte[] key);

    void putUntracked(byte[] key, byte[] value);

    <G> ResourceIterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor);

    GraknException exception(String message);

    GraknException exception(Exception message);

    GraknException exception(GraknException message);

    interface Schema extends Storage {

        void incrementReference();

        void decrementReference();

        void mayRefresh();
    }
}
