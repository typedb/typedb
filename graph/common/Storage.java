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

package grakn.core.graph.common;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;

import java.util.function.BiFunction;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Storage {

    boolean isOpen();

    byte[] get(byte[] key);

    byte[] getLastKey(byte[] prefix);

    void delete(byte[] key);

    void put(byte[] key);

    void put(byte[] key, byte[] value);

    void putUntracked(byte[] key);

    void putUntracked(byte[] key, byte[] value);

    void mergeUntracked(byte[] key, byte[] value);

    void setModified(byte[] key);

    <G> FunctionalIterator<G> iterate(byte[] key, BiFunction<byte[], byte[], G> constructor);

    GraknException exception(ErrorMessage error);

    GraknException exception(Exception exception);

    void close();

    default boolean isSchema() { return false; }

    default Schema asSchema() {
        throw exception(GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Schema.class)));
    }

    interface Schema extends Storage {

        KeyGenerator.Schema schemaKeyGenerator();

        default boolean isSchema() { return true; }

        default Schema asSchema() { return this; }
    }

    interface Data extends Storage {

        KeyGenerator.Data dataKeyGenerator();
    }
}
