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

package grakn.core.graph.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.SupportsNullSerializer;

import java.lang.reflect.Array;

public abstract class ArraySerializer implements SupportsNullSerializer {

    protected abstract Object getArray(int length);

    protected abstract void setArray(Object array, int pos, Object value);

    <T> T convertInternal(Object value, Class primitiveClass, Class boxedClass) {
        int size;
        if (value == null) {
            return null;
        }
        if ((value.getClass().isArray()) && (value.getClass().getComponentType().equals(primitiveClass))) {
            //primitive array of the right type
            return (T) value;
        } else if ((size = isIterableOf(value, boxedClass)) >= 0) {
            //Iterable of the right (boxed) type and no null values
            Object array = getArray(size);
            int pos = 0;
            for (Object element : (Iterable) value) {
                setArray(array, pos++, element);
            }
            return (T) array;
        } else if ((size = isArrayOf(value, boxedClass)) >= 0) {
            //array of the right (boxed) type and no null values
            Object array = getArray(size);
            for (int i = 0; i < size; i++) {
                setArray(array, i, Array.get(value, i));
            }
            return (T) array;
        }
        return null;
    }

    private int isIterableOf(Object value, Class boxedClass) {
        if (!(value instanceof Iterable)) return -1;
        Iterable c = (Iterable) value;
        int size = 0;
        for (Object element : c) {
            if (element == null || !element.getClass().equals(boxedClass)) return -1;
            size++;
        }
        return size;
    }

    private int isArrayOf(Object value, Class boxedClass) {
        if (!value.getClass().isArray() || !value.getClass().getComponentType().equals(boxedClass)) {
            return -1;
        }
        for (int i = 0; i < Array.getLength(value); i++) {
            if (Array.get(value, i) == null) return -1;
        }
        return Array.getLength(value);
    }

    //############### Serialization ###################

    protected int getLength(ScanBuffer buffer) {
        long length = VariableLong.readPositive(buffer) - 1;
        Preconditions.checkArgument(length >= -1 && length <= Integer.MAX_VALUE);
        return (int) length;
    }

    protected void writeLength(WriteBuffer buffer, Object array) {
        if (array == null) VariableLong.writePositive(buffer, 0);
        else {
            long length = ((long) Array.getLength(array)) + 1;
            VariableLong.writePositive(buffer, length);
        }
    }

}
