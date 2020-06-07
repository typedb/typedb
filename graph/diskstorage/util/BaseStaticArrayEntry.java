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

package grakn.core.graph.diskstorage.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.graphdb.relations.RelationCache;

import java.util.Map;

public class BaseStaticArrayEntry extends StaticArrayBuffer implements Entry {

    private final int valuePosition;

    public BaseStaticArrayEntry(byte[] array, int offset, int limit, int valuePosition) {
        super(array, offset, limit);
        Preconditions.checkArgument(valuePosition > 0);
        Preconditions.checkArgument(valuePosition <= length());
        this.valuePosition = valuePosition;
    }

    public BaseStaticArrayEntry(byte[] array, int limit, int valuePosition) {
        this(array, 0, limit, valuePosition);
    }

    public BaseStaticArrayEntry(byte[] array, int valuePosition) {
        this(array, 0, array.length, valuePosition);
    }

    public BaseStaticArrayEntry(StaticBuffer buffer, int valuePosition) {
        super(buffer);
        Preconditions.checkArgument(valuePosition > 0);
        Preconditions.checkArgument(valuePosition <= length());
        this.valuePosition = valuePosition;
    }

    @Override
    public int getValuePosition() {
        return valuePosition;
    }

    @Override
    public boolean hasValue() {
        return valuePosition < length();
    }

    @Override
    public StaticBuffer getColumn() {
        return getColumnAs(StaticBuffer.STATIC_FACTORY);
    }

    @Override
    public <T> T getColumnAs(Factory<T> factory) {
        return super.as(factory, 0, valuePosition);
    }

    @Override
    public StaticBuffer getValue() {
        return getValueAs(StaticBuffer.STATIC_FACTORY);
    }

    @Override
    public <T> T getValueAs(Factory<T> factory) {
        return super.as(factory, valuePosition, super.length() - valuePosition);
    }

    //Override from StaticArrayBuffer to restrict to column

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof StaticBuffer)) return false;
        final Entry b = (Entry) o;
        return getValuePosition() == b.getValuePosition() && compareTo(getValuePosition(), b, getValuePosition()) == 0;
    }

    @Override
    public int hashCode() {
        return hashCode(getValuePosition());
    }

    @Override
    public int compareTo(StaticBuffer other) {
        int otherLen = (other instanceof Entry) ? ((Entry) other).getValuePosition() : other.length();
        return compareTo(getValuePosition(), other, otherLen);
    }

    @Override
    public String toString() {
        String s = super.toString();
        int pos = getValuePosition() * 4;
        return s.substring(0, pos - 1) + "->" + (getValuePosition() < length() ? s.substring(pos) : "");
    }

    //########## CACHE ############

    @Override
    public RelationCache getCache() {
        return null;
    }

    @Override
    public void setCache(RelationCache cache) {
        throw new UnsupportedOperationException();
    }

    //########## META DATA ############

    @Override
    public boolean hasMetaData() {
        return false;
    }

    @Override
    public Map<EntryMetaData, Object> getMetaData() {
        return EntryMetaData.EMPTY_METADATA;
    }

}
