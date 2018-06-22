/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package pick;

import java.util.HashSet;
import java.util.Random;
import java.util.stream.Stream;

/**
 *
 */
public class RandomOffsetGenerator {

    public static Stream<Integer> generate(Random rand, int offsetBound) {

        HashSet<Object> previousRandomOffsets = new HashSet<>();

        return Stream.generate(() -> {

            boolean foundUnique = false;

            int nextChoice = 0;
            while (!foundUnique) {
                nextChoice = rand.nextInt(offsetBound);
                foundUnique = previousRandomOffsets.add(nextChoice);
            }
            return nextChoice;
        }).limit(offsetBound);
    }
}
