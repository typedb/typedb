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

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.VertexLabel;


public interface TypeInspector {

    default PropertyKey getExistingPropertyKey(long id) {
        return (PropertyKey)getExistingRelationType(id);
    }

    default EdgeLabel getExistingEdgeLabel(long id) {
        return (EdgeLabel)getExistingRelationType(id);
    }

    RelationType getExistingRelationType(long id);

    VertexLabel getExistingVertexLabel(long id);

    boolean containsRelationType(String name);

    RelationType getRelationType(String name);

}
