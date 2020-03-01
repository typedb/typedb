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
import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.database.serialize.SerializerInjected;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeDefinitionDescription;


public class TypeDefinitionDescriptionSerializer implements AttributeSerializer<TypeDefinitionDescription>, SerializerInjected {

    private Serializer serializer;

    @Override
    public TypeDefinitionDescription read(ScanBuffer buffer) {
        TypeDefinitionCategory defCategory = serializer.readObjectNotNull(buffer, TypeDefinitionCategory.class);
        Object modifier = serializer.readClassAndObject(buffer);
        return new TypeDefinitionDescription(defCategory,modifier);
    }

    @Override
    public void write(WriteBuffer buffer, TypeDefinitionDescription attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getCategory());
        out.writeClassAndObject(attribute.getModifier());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
