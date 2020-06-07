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

/**
 * A JanusGraphSchemaType is a JanusGraphSchemaElement that represents a label or key
 * used in the graph. As such, a schema type is either a RelationType
 * or a VertexLabel.
 * <p>
 * JanusGraphSchemaTypes are a special JanusGraphSchemaElement in that they are referenced from the
 * main graph when creating vertices, edges, and properties.
 */
public interface JanusGraphSchemaType extends JanusGraphSchemaElement {

    /**
     * Checks whether this schema type has been newly created in the current transaction.
     *
     * @return True, if the schema type has been newly created, else false.
     */
    boolean isNew();
}
