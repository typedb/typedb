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

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class DoubleArraySerializer extends ArraySerializer implements AttributeSerializer<double[]> {

    @Override
    public double[] convert(Object value) {
        return convertInternal(value, double.class, Double.class);
    }

    @Override
    protected Object getArray(int length) {
        return new double[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setDouble(array, pos, ((Double) value));
    }

    //############### Serialization ###################

    @Override
    public double[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length < 0) return null;
        return buffer.getDoubles(length);
    }

    @Override
    public void write(WriteBuffer buffer, double[] attribute) {
        writeLength(buffer, attribute);
        if (attribute != null) {
            for (double anAttribute : attribute) {
                buffer.putDouble(anAttribute);
            }
        }
    }
}
