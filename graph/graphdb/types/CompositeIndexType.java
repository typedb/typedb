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

package grakn.core.graph.graphdb.types;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;


public interface CompositeIndexType extends IndexType {

    long getID();

    IndexField[] getFieldKeys();

    SchemaStatus getStatus();

    /*
     * single == unique,
     */
    Cardinality getCardinality();

    ConsistencyModifier getConsistencyModifier();
}
