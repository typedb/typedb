/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.iterator.RelationIterator;

import java.util.Objects;

public class RelationTraversal extends Traversal {

    public RelationTraversal() {
        super();
    }

    FunctionalIterator<VertexMap> relations(GraphManager graphMgr) {
        return new RelationIterator(this, graphMgr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationTraversal that = (RelationTraversal) o;
        return (this.structure.equals(that.structure) && this.parameters.equals(that.parameters));
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.structure, this.parameters);
    }
}
