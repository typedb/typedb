/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.encoding;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.encoding.key.Key;
import com.vaticle.typedb.core.encoding.key.KeyGenerator;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Storage {

    boolean isOpen();

    boolean isReadOnly();

    ByteArray get(Key key);

    <T extends Key> T getLastKey(Key.Prefix<T> key);

    void deleteUntracked(Key key);

    <T extends Key> Forwardable<KeyValue<T, ByteArray>, Order.Asc> iterate(Key.Prefix<T> key);

    <T extends Key, ORDER extends Order> Forwardable<KeyValue<T, ByteArray>, ORDER> iterate(Key.Prefix<T> key, ORDER order);

    void putUntracked(Key key);

    void putUntracked(Key key, ByteArray value);

    TypeDBException exception(ErrorMessage error);

    TypeDBException exception(Exception exception);

    void close();

    default boolean isSchema() {
        return false;
    }

    default Schema asSchema() {
        throw exception(TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Schema.class)));
    }

    interface Schema extends Storage {

        KeyGenerator.Schema schemaKeyGenerator();

        default boolean isSchema() {
            return true;
        }

        default Schema asSchema() {
            return this;
        }
    }

    interface Data extends Storage {

        KeyGenerator.Data dataKeyGenerator();

        void putTracked(Key key);

        void putTracked(Key key, ByteArray value);

        void deleteTracked(Key key);

        void mergeUntracked(Key key, ByteArray value);

        // TODO: investigate why replacing ByteArray with Key for tracking makes navigable set intersection super slow
        void trackModified(ByteArray key);

        void trackExclusiveBytes(ByteArray bytes);
    }

}
