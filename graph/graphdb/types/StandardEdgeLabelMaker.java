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
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.EdgeLabelMaker;
import grakn.core.graph.graphdb.database.serialize.AttributeHandler;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.structure.Direction;

import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.INVISIBLE;
import static grakn.core.graph.graphdb.types.TypeDefinitionCategory.UNIDIRECTIONAL;


public class StandardEdgeLabelMaker extends StandardRelationTypeMaker implements EdgeLabelMaker {

    private Direction unidirectionality;

    public StandardEdgeLabelMaker(StandardJanusGraphTx tx, String name, AttributeHandler attributeHandler) {
        super(tx, name, attributeHandler);
        unidirectionality = Direction.BOTH;
    }

    @Override
    JanusGraphSchemaCategory getSchemaCategory() {
        return JanusGraphSchemaCategory.EDGELABEL;
    }

    @Override
    public StandardEdgeLabelMaker directed() {
        unidirectionality = Direction.BOTH;
        return this;
    }

    @Override
    public StandardEdgeLabelMaker unidirected() {
        return unidirected(Direction.OUT);
    }

    public StandardEdgeLabelMaker unidirected(Direction dir) {
        unidirectionality = Preconditions.checkNotNull(dir);
        return this;
    }

    @Override
    public StandardEdgeLabelMaker multiplicity(Multiplicity multiplicity) {
        super.multiplicity(multiplicity);
        return this;
    }

    @Override
    public StandardEdgeLabelMaker signature(PropertyKey... types) {
        super.signature(types);
        return this;
    }

    @Override
    public StandardEdgeLabelMaker sortKey(PropertyKey... types) {
        super.sortKey(types);
        return this;
    }

    @Override
    public StandardEdgeLabelMaker sortOrder(Order order) {
        super.sortOrder(order);
        return this;
    }

    @Override
    public StandardEdgeLabelMaker invisible() {
        super.invisible();
        return this;
    }

    @Override
    public EdgeLabel make() {
        TypeDefinitionMap definition = makeDefinition();
        Preconditions.checkArgument(unidirectionality == Direction.BOTH || !getMultiplicity().isUnique(unidirectionality.opposite()),
                "Unidirectional labels cannot have restricted multiplicity at the other end");
        Preconditions.checkArgument(unidirectionality == Direction.BOTH || !hasSortKey() ||
                        !getMultiplicity().isUnique(unidirectionality),
                "Unidirectional labels with restricted multiplicity cannot have a sort key");
        Preconditions.checkArgument(unidirectionality != Direction.IN || definition.getValue(INVISIBLE, Boolean.class));

        definition.setValue(UNIDIRECTIONAL, unidirectionality);
        return tx.makeEdgeLabel(getName(), definition);
    }

}
