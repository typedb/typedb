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

package grakn.core.graph.graphdb.database.idassigner;

import com.google.common.base.Preconditions;
import grakn.core.graph.graphdb.database.idassigner.IDBlockSizer;


public class StaticIDBlockSizer implements IDBlockSizer {

    private final long blockSize;
    private final long blockSizeLimit;

    public StaticIDBlockSizer(long blockSize, long blockSizeLimit) {
        Preconditions.checkArgument(blockSize > 0);
        Preconditions.checkArgument(blockSizeLimit > 0);
        Preconditions.checkArgument(blockSizeLimit > blockSize,"%s vs %s",blockSizeLimit,blockSize);
        this.blockSize = blockSize;
        this.blockSizeLimit = blockSizeLimit;
    }

    @Override
    public long getBlockSize(int idNamespace) {
        return blockSize;
    }

    @Override
    public long getIdUpperBound(int idNamespace) {
        return blockSizeLimit;
    }
}
