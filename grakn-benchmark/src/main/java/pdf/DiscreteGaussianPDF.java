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

/**
 *
 */
public class DiscreteGaussianPDF extends PDF {
    private Random rand;
    private Double mean;
    private Double variance;

    /**
     * @param rand
     * @param mean
     * @param variance
     */
    public DiscreteGaussianPDF(Random rand, Double mean, Double variance) {
        this.rand = rand;
        this.mean = mean;
        this.variance = variance;
    }

    /**
     * @return
     */
    public int next() {
        double z = rand.nextGaussian();
        return (int) (Math.pow(variance, 0.5) * z + mean);
    }
}