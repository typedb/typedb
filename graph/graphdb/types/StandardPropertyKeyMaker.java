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

package grakn.core.graph.graphdb.types;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.PropertyKeyMaker;
import grakn.core.graph.graphdb.database.serialize.AttributeHandler;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

import java.lang.reflect.Modifier;

import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.DATATYPE;


public class StandardPropertyKeyMaker extends StandardRelationTypeMaker implements PropertyKeyMaker {

    private Class<?> dataType;

    public StandardPropertyKeyMaker(StandardJanusGraphTx tx, String name, AttributeHandler attributeHandler) {
        super(tx, name, attributeHandler);
        dataType = null;
        cardinality(Cardinality.SINGLE);
    }

    @Override
    JanusGraphSchemaCategory getSchemaCategory() {
        return JanusGraphSchemaCategory.PROPERTYKEY;
    }

    @Override
    public StandardPropertyKeyMaker dataType(Class<?> clazz) {
        Preconditions.checkArgument(clazz != null, "Need to specify a data type");
        dataType = clazz;
        return this;
    }

    @Override
    public StandardPropertyKeyMaker cardinality(Cardinality cardinality) {
        super.multiplicity(Multiplicity.convert(cardinality));
        return this;
    }


    @Override
    public StandardPropertyKeyMaker invisible() {
        super.invisible();
        return this;
    }

    @Override
    public StandardPropertyKeyMaker signature(PropertyKey... types) {
        super.signature(types);
        return this;
    }

    @Override
    public StandardPropertyKeyMaker sortKey(PropertyKey... types) {
        super.sortKey(types);
        return this;
    }

    @Override
    public StandardPropertyKeyMaker sortOrder(Order order) {
        super.sortOrder(order);
        return this;
    }

    @Override
    public PropertyKey make() {
        Preconditions.checkArgument(dataType != null, "Need to specify a datatype");
        Preconditions.checkArgument(tx.validDataType(dataType), "Not a supported data type: %s", dataType);
        Preconditions.checkArgument(!dataType.isPrimitive(), "Primitive types are not supported. Use the corresponding object type, e.g. Integer.class instead of int.class [%s]", dataType);
        Preconditions.checkArgument(!dataType.isInterface(), "Datatype must be a class and not an interface: %s", dataType);
        Preconditions.checkArgument(dataType.isArray() || !Modifier.isAbstract(dataType.getModifiers()), "Datatype cannot be an abstract class: %s", dataType);

        TypeDefinitionMap definition = makeDefinition();
        definition.setValue(DATATYPE, dataType);
        return tx.makePropertyKey(getName(), definition);
    }
}
