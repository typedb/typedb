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

package grakn.core.graph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;

/**
 * Simple implementation of PartitionAssignment.
 *
 */
public class SimplePartitionAssignment implements PartitionAssignment {

    private int partitionID;

    public SimplePartitionAssignment() {
    }

    public SimplePartitionAssignment(int id) {
        setPartitionID(id);
    }

    public void setPartitionID(int id) {
        Preconditions.checkArgument(id >= 0);
        this.partitionID = id;
    }

    @Override
    public int getPartitionID() {
        return partitionID;
    }
}
