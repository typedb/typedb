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

package strategy;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * @param <T>
 */
//TODO Shouldn't have name including "Collection"?
public class RouletteWheelCollection<T> implements PickableCollection {
    private final NavigableMap<Double, T> map = new TreeMap<Double, T>();
    private final Random random;
    private double total = 0;

    public RouletteWheelCollection(Random random) {
        this.random = random;
    }

    public RouletteWheelCollection<T> add(double weight, T result) {
        if (weight <= 0) return this;
        total += weight;
        map.put(total, result);
        return this;
    }

    public T next() {
        double value = random.nextDouble() * total;
        return map.higherEntry(value).getValue();
    }
}