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
import com.google.common.collect.Sets;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.RelationTypeMaker;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.database.serialize.AttributeHandler;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.system.SystemTypeManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.INVISIBLE;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.MULTIPLICITY;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.SIGNATURE;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.SORT_KEY;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.SORT_ORDER;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.STATUS;

public abstract class StandardRelationTypeMaker implements RelationTypeMaker {

    protected final StandardJanusGraphTx tx;
    private final AttributeHandler attributeHandler;

    private String name;
    private boolean isInvisible;
    private final List<PropertyKey> sortKey;
    private Order sortOrder;
    private final List<PropertyKey> signature;
    private Multiplicity multiplicity;
    private SchemaStatus status = SchemaStatus.ENABLED;

    StandardRelationTypeMaker(StandardJanusGraphTx tx, String name, AttributeHandler attributeHandler) {
        this.tx = Preconditions.checkNotNull(tx);
        this.attributeHandler = Preconditions.checkNotNull(attributeHandler);
        name(name);

        //Default assignments
        isInvisible = false;
        sortKey = new ArrayList<>(4);
        sortOrder = Order.ASC;
        signature = new ArrayList<>(4);
        multiplicity = Multiplicity.MULTI;
    }


    public String getName() {
        return this.name;
    }

    protected boolean hasSortKey() {
        return !sortKey.isEmpty();
    }

    protected Multiplicity getMultiplicity() {
        return multiplicity;
    }

    abstract JanusGraphSchemaCategory getSchemaCategory();

    private void checkGeneralArguments() {
        checkSortKey(sortKey);
        Preconditions.checkArgument(sortOrder == Order.ASC || hasSortKey(), "Must define a sort key to use ordering");
        checkSignature(signature);
        Preconditions.checkArgument(Sets.intersection(Sets.newHashSet(sortKey), Sets.newHashSet(signature)).isEmpty(),
                "Signature and sort key must be disjoined");
        Preconditions.checkArgument(!hasSortKey() || !multiplicity.isConstrained(), "Cannot define a sort-key on constrained edge labels");
    }

    private long[] checkSortKey(List<PropertyKey> sig) {
        for (PropertyKey key : sig) {
            Preconditions.checkArgument(attributeHandler.isOrderPreservingDatatype(key.dataType()),
                    "Key must have an order-preserving data type to be used as sort key: " + key);
        }
        return checkSignature(sig);
    }

    private static long[] checkSignature(List<PropertyKey> sig) {
        Preconditions.checkArgument(sig.size() == (Sets.newHashSet(sig)).size(), "Signature and sort key cannot contain duplicate types");
        long[] signature = new long[sig.size()];
        for (int i = 0; i < sig.size(); i++) {
            PropertyKey key = sig.get(i);
            Preconditions.checkNotNull(key);
            Preconditions.checkArgument(!((PropertyKey) key).dataType().equals(Object.class),
                    "Signature and sort keys must have a proper declared datatype: %s", key.name());
            signature[i] = key.longId();
        }
        return signature;
    }

    protected final TypeDefinitionMap makeDefinition() {
        checkGeneralArguments();

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(INVISIBLE, isInvisible);
        def.setValue(SORT_KEY, checkSortKey(sortKey));
        def.setValue(SORT_ORDER, sortOrder);
        def.setValue(SIGNATURE, checkSignature(signature));
        def.setValue(MULTIPLICITY, multiplicity);
        def.setValue(STATUS, status);
        return def;
    }

    public StandardRelationTypeMaker multiplicity(Multiplicity multiplicity) {
        this.multiplicity = Preconditions.checkNotNull(multiplicity);
        return this;
    }

    @Override
    public StandardRelationTypeMaker signature(PropertyKey... types) {
        Preconditions.checkArgument(types != null && types.length > 0);
        signature.addAll(Arrays.asList(types));
        return this;
    }

    public StandardRelationTypeMaker status(SchemaStatus status) {
        this.status = Preconditions.checkNotNull(status);
        return this;
    }

    /**
     * Configures the composite sort key for this label.
     * <p>
     * Specifying the sort key of a type allows relations of this type to be efficiently retrieved in the order of
     * the sort key.
     * <br>
     * For instance, if the edge label <i>friend</i> has the sort key (<i>since</i>), which is a property key
     * with a timestamp data type, then one can efficiently retrieve all edges with label <i>friend</i> in a specified
     * time interval using JanusGraphVertexQuery#interval.
     * <br>
     * In other words, relations are stored on disk in the order of the configured sort key. The sort key is empty
     * by default.
     * <br>
     * If multiple types are specified as sort key, then those are considered as a <i>composite</i> sort key, i.e. taken jointly
     * in the given order.
     * <p>
     * RelationTypes used in the sort key must be either property out-unique keys or out-unique unidirected edge lables.
     *
     * @param keys JanusGraphTypes composing the sort key. The order is relevant.
     * @return this LabelMaker
     */
    public StandardRelationTypeMaker sortKey(PropertyKey... keys) {
        Preconditions.checkArgument(keys != null && keys.length > 0);
        sortKey.addAll(Arrays.asList(keys));
        return this;
    }

    /**
     * Defines in which order to sort the relations for efficient retrieval, i.e. either increasing (Order#ASC) or
     * decreasing (Order#DESC).
     * <p>
     * Note, that only one sort order can be specified and that a sort key must be defined to use a sort order.
     *
     * see #sortKey(PropertyKey... keys)
     */
    public StandardRelationTypeMaker sortOrder(Order order) {
        this.sortOrder = Preconditions.checkNotNull(order);
        return this;
    }

    public StandardRelationTypeMaker name(String name) {
        SystemTypeManager.throwIfSystemName(getSchemaCategory(), name);
        this.name = name;
        return this;
    }

    public StandardRelationTypeMaker invisible() {
        this.isInvisible = true;
        return this;
    }


}
