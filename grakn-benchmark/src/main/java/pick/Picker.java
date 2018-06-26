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

import ai.grakn.GraknTx;

import java.util.Random;
import java.util.stream.Stream;

/**
 * @param <T>
 */
public abstract class Picker<T> implements PickerInterface<T> {
    protected Random rand;

    public Picker(Random rand) {
        this.rand = rand;
    }

    public Stream<Integer> getRandomOffsetStream(int streamLength, GraknTx tx) {
        int typeCount = getConceptCount(tx);

        // If there aren't enough concepts to fulfill the number requested, then return null
        if (typeCount < streamLength) return null;

        return RandomOffsetGenerator.generate(this.rand, typeCount);
    }

    abstract Integer getConceptCount(GraknTx tx);
}
