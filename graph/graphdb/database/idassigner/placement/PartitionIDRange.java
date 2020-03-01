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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * An instance of this class describes a range of partition ids. This range if defined by a lower id (inclusive)
 * and upper id (exclusive). When lowerId &lt; upperId this partition range is called a proper range since it describes the
 * contiguous block of ids from lowerId until upperId. When lowerId &gt;= upperID then the partition block "wraps around" the
 * specified idUpperBound. In other words, it describes the ids from [lowerId,idUpperBound) AND [0,upperId).
 * <p>
 * It is always true that lowerID and upperID are smaller or equal than idUpperBound.
 */
public class PartitionIDRange {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionIDRange.class);
    private static final Random RANDOM = new Random();

    private final int lowerID;
    private final int upperID;
    private final int idUpperBound;


    public PartitionIDRange(int lowerID, int upperID, int idUpperBound) {
        Preconditions.checkArgument(idUpperBound > 0, "Partition limit " + idUpperBound + " must be positive");
        Preconditions.checkArgument(lowerID >= 0, "Negative partition lower bound " + lowerID);
        Preconditions.checkArgument(lowerID < idUpperBound, "Partition lower bound " + lowerID + " exceeds limit " + idUpperBound);
        Preconditions.checkArgument(upperID >= 0, "Negative partition upper bound " + upperID);
        Preconditions.checkArgument(upperID <= idUpperBound, "Partition upper bound " + upperID + " exceeds limit " + idUpperBound);
        this.lowerID = lowerID;
        this.upperID = upperID;
        this.idUpperBound = idUpperBound;
    }

    public int getLowerID() {
        return lowerID;
    }

    public int getUpperID() {
        return upperID;
    }

    public int getIdUpperBound() {
        return idUpperBound;
    }

    public int[] getAllContainedIDs() {
        int[] result;
        if (lowerID < upperID) { //"Proper" id range
            result = new int[upperID - lowerID];
            int pos = 0;
            for (int id = lowerID; id < upperID; id++) {
                result[pos++] = id;
            }
        } else { //Id range "wraps around"
            result = new int[(idUpperBound - lowerID) + (upperID)];
            int pos = 0;
            for (int id = 0; id < upperID; id++) {
                result[pos++] = id;
            }
            for (int id = lowerID; id < idUpperBound; id++) {
                result[pos++] = id;
            }
        }
        return result;
    }

    /**
     * Returns true of the given partitionId lies within this partition id range, else false.
     */
    public boolean contains(int partitionId) {
        if (lowerID < upperID) { //"Proper" id range
            return lowerID <= partitionId && upperID > partitionId;
        } else { //Id range "wraps around"
            return (lowerID <= partitionId && partitionId < idUpperBound) ||
                    (upperID > partitionId && partitionId >= 0);
        }
    }

    @Override
    public String toString() {
        return "[" + lowerID + "," + upperID + ")%" + idUpperBound;
    }

    /**
     * Returns a RANDOM partition id that lies within this partition id range.
     */
    public int getRandomID() {
        //Compute the width of the partition...
        int partitionWidth;
        if (lowerID < upperID) partitionWidth = upperID - lowerID; //... for "proper" ranges
        else partitionWidth = (idUpperBound - lowerID) + upperID; //... and those that "wrap around"
        Preconditions.checkArgument(partitionWidth > 0, partitionWidth);
        return (RANDOM.nextInt(partitionWidth) + lowerID) % idUpperBound;
    }

    /*
    =========== Helper methods to generate PartitionIDRanges ============
     */

    public static List<PartitionIDRange> getGlobalRange(int partitionBits) {
        Preconditions.checkArgument(partitionBits >= 0 && partitionBits < (Integer.SIZE - 1), "Invalid partition bits: %s", partitionBits);
        final int partitionIdBound = (1 << (partitionBits));
        return ImmutableList.of(new PartitionIDRange(0, partitionIdBound, partitionIdBound));
    }

    public static List<PartitionIDRange> getIDRanges(int partitionBits, List<KeyRange> locals) {
        Preconditions.checkArgument(partitionBits > 0 && partitionBits < (Integer.SIZE - 1));
        Preconditions.checkArgument(locals != null && !locals.isEmpty(), "KeyRanges are empty");
        int partitionIdBound = (1 << (partitionBits));
        int backShift = Integer.SIZE - partitionBits;
        List<PartitionIDRange> partitionRanges = Lists.newArrayList();
        for (KeyRange local : locals) {
            Preconditions.checkArgument(local.getStart().length() >= 4);
            Preconditions.checkArgument(local.getEnd().length() >= 4);
            if (local.getStart().equals(local.getEnd())) { //Start=End => Partition spans entire range
                partitionRanges.add(new PartitionIDRange(0, partitionIdBound, partitionIdBound));
                continue;
            }

            int startInt = local.getStart().getInt(0);
            int lowerID = startInt >>> backShift;
            //Lower id must be inclusive, so check that we did not truncate anything!
            boolean truncatedBits = (lowerID << backShift) != startInt;
            StaticBuffer start = local.getAt(0);
            for (int i = 4; i < start.length() && !truncatedBits; i++) {
                if (start.getByte(i) != 0) truncatedBits = true;
            }
            if (truncatedBits) lowerID += 1; //adjust to make sure we are inclusive
            int upperID = local.getEnd().getInt(0) >>> backShift; //upper id is exclusive
            //Check that we haven't jumped order indicating that the interval was too small
            if ((local.getStart().compareTo(local.getEnd()) < 0 && lowerID >= upperID)) {
                discardRange(local);
                continue;
            }
            lowerID = lowerID % partitionIdBound; //ensure that lowerID remains within range
            if (lowerID == upperID) { //After re-normalizing, check for interval collision
                discardRange(local);
                continue;
            }
            partitionRanges.add(new PartitionIDRange(lowerID, upperID, partitionIdBound));
        }
        return partitionRanges;
    }


    private static void discardRange(KeyRange local) {
        LOG.warn("Individual key range is too small for partition block - result would be empty; hence ignored: {}", local);
    }

}
