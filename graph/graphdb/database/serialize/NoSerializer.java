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

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;


public class NoSerializer<V> implements AttributeSerializer<V> {

    private final Class<V> datatype;

    public NoSerializer(Class<V> datatype) {
        this.datatype = datatype;
    }

    private IllegalArgumentException error() {
        return new IllegalArgumentException("Serializing objects of type ["+datatype+"] is not supported");
    }

    @Override
    public V read(ScanBuffer buffer) {
        throw error();
    }

    @Override
    public void write(WriteBuffer buffer, V attribute) {
        throw error();
    }

    @Override
    public void verifyAttribute(V value) {
        throw error();
    }

    @Override
    public V convert(Object value) {
        throw error();
    }

}
