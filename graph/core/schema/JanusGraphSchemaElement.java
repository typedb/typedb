/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import grakn.core.graph.core.Namifiable;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.core.schema.RelationTypeIndex;

/**
 * Marks any element that is part of a JanusGraph Schema.
 * JanusGraph Schema elements can be uniquely identified by their name.
 * <p>
 * A JanusGraph Schema element is either a {@link JanusGraphSchemaType} or an index definition, i.e.
 * {@link JanusGraphIndex} or {@link RelationTypeIndex}.
 *
 */
public interface JanusGraphSchemaElement extends Namifiable {

}
