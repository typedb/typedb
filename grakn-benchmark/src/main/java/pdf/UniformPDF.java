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

package pdf;

import java.util.Random;
import java.util.stream.IntStream;

/**
 *
 */
public class UniformPDF extends PDF {

    private Random rand;
    private int lowerBound;
    private int upperBound;

    /**
     * @param rand
     * @param lowerBound
     * @param upperBound
     */
    public UniformPDF(Random rand, int lowerBound, int upperBound) {
        this.rand = rand;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /**
     * @return
     */
    @Override
    public int next() {
        IntStream intStream = rand.ints(1, lowerBound, upperBound + 1);
        return intStream.findFirst().getAsInt();
    }
}
