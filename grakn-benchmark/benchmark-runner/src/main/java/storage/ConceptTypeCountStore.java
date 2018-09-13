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

package storage;

import ai.grakn.concept.Concept;

import java.util.HashMap;

/**
 *
 */
public class ConceptTypeCountStore implements ConceptStore {

    private final HashMap<String, Integer> conceptTypeCountStorage;

    public ConceptTypeCountStore() {
        this.conceptTypeCountStorage = new HashMap<>();
    }

    @Override
    public void add(Concept concept) {
        String typeLabel = concept.asThing().type().label().toString();

//        Integer count = this.conceptTypeCountStorage.get(typeLabel);
//        this.conceptTypeCountStorage.put(typeLabel, count + 1);
        this.conceptTypeCountStorage.putIfAbsent(typeLabel, 0);
        this.conceptTypeCountStorage.compute(typeLabel, (k, v) -> v + 1);
    }

    public int get(String typeLabel) {
        return this.conceptTypeCountStorage.getOrDefault(typeLabel, 0);
    }

    public int total() {
        /*
        Return the total number of concepts
         */
        int total = 0;
        for (int c : conceptTypeCountStorage.values()) {
            total += c;
        }
        return total;
    }
}
