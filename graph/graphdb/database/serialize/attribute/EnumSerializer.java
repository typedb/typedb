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
import grakn.core.graph.graphdb.database.serialize.OrderPreservingSerializer;

public class EnumSerializer<E extends Enum> implements OrderPreservingSerializer<E> {

    private static final long serialVersionUID = 117423419862504186L;

    private final Class<E> datatype;
    private final IntegerSerializer ints = new IntegerSerializer();

    public EnumSerializer(Class<E> datatype) {
        Preconditions.checkArgument(datatype != null && datatype.isEnum());
        this.datatype = datatype;
    }

    private E getValue(long ordinal) {
        E[] values = datatype.getEnumConstants();
        Preconditions.checkArgument(ordinal>=0 && ordinal<values.length,"Invalid ordinal number (max %s): %s",values.length,ordinal);
        return values[(int)ordinal];
    }

    @Override
    public E read(ScanBuffer buffer) {
        return getValue(VariableLong.readPositive(buffer));
    }

    @Override
    public void write(WriteBuffer out, E object) {
        VariableLong.writePositive(out, object.ordinal());
    }

    @Override
    public E readByteOrder(ScanBuffer buffer) {
        return getValue(ints.readByteOrder(buffer));
    }


    @Override
    public void writeByteOrder(WriteBuffer buffer, E attribute) {
        ints.writeByteOrder(buffer,attribute.ordinal());
    }

}
