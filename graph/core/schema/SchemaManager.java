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

package grakn.core.graph.core.schema;

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;

public interface SchemaManager extends SchemaInspector {

    /**
     * Returns a PropertyKeyMaker instance to define a new PropertyKey with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the key and associated consistency constraints.
     * <p>
     * The key constructed with this maker will be created in the context of this transaction.
     *
     * @return a PropertyKeyMaker linked to this transaction.
     * see PropertyKeyMaker
     * see PropertyKey
     */
    PropertyKeyMaker makePropertyKey(String name);

    /**
     * Returns a EdgeLabelMaker instance to define a new EdgeLabel with the given name.
     * By defining types explicitly (rather than implicitly through usage) one can control various
     * aspects of the label and associated consistency constraints.
     * <p>
     * The label constructed with this maker will be created in the context of this transaction.
     *
     * @return a EdgeLabelMaker linked to this transaction.
     * see EdgeLabelMaker
     * see EdgeLabel
     */
    EdgeLabelMaker makeEdgeLabel(String name);

    /**
     * Returns a VertexLabelMaker to define a new vertex label with the given name. Note, that the name must
     * be unique.
     */
    VertexLabelMaker makeVertexLabel(String name);

    /**
     * Add property constraints for a given vertex label.
     *
     * @param vertexLabel to which the constraints applies.
     * @param keys        defines the properties which should be added to the VertexLabel as constraints.
     * @return a VertexLabel edited which contains the added constraints.
     */
    VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys);

    /**
     * Add property constraints for a given edge label.
     *
     * @param edgeLabel to which the constraints applies.
     * @param keys      defines the properties which should be added to the EdgeLabel as constraints.
     * @return a EdgeLabel edited which contains the added constraints.
     */
    EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys);

    /**
     * Add a constraint on which vertices the given edge label can connect.
     *
     * @param edgeLabel to which the constraint applies.
     * @param outVLabel specifies the outgoing vertex for this connection.
     * @param inVLabel  specifies the incoming vertex for this connection.
     * @return a EdgeLabel edited which contains the added constraint.
     */
    EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel);


}
