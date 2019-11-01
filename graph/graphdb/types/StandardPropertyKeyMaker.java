// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
