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

package grakn.core.graph.graphdb.types.system;

import com.google.common.collect.ImmutableSet;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.types.IndexType;

import java.util.Collections;


public abstract class EmptyRelationType extends EmptyVertex implements InternalRelationType {

    @Override
    public boolean isInvisible() {
        return true;
    }

    @Override
    public long[] getSignature() {
        return new long[0];
    }

    @Override
    public long[] getSortKey() {
        return new long[0];
    }

    @Override
    public Order getSortOrder() {
        return Order.ASC;
    }

    @Override
    public InternalRelationType getBaseType() {
        return null;
    }

    @Override
    public Iterable<InternalRelationType> getRelationIndexes() {
        return ImmutableSet.of(this);
    }

    @Override
    public SchemaStatus getStatus() {
        return SchemaStatus.ENABLED;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        return Collections.EMPTY_LIST;
    }

    public Integer getTTL() {
        return 0;
    }

    @Override
    public String toString() {
        return name();
    }
}
