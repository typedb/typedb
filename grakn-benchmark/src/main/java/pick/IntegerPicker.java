/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package pick;

import ai.grakn.GraknTx;

import java.util.Random;
import java.util.stream.Stream;

/**
 *
 */
public class IntegerPicker implements StreamInterface<Integer>{
    private Random rand;

    public IntegerPicker(Random rand, int lowerBound, int upperBound) {
        this.rand = rand;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    private int lowerBound;
    private int upperBound;

    @Override
    public Stream<Integer> getStream(int streamLength, GraknTx tx) {
        return rand.ints(lowerBound, upperBound + 1).boxed();
    }
}
