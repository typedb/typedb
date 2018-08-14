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
import storage.IdStoreInterface;

import java.util.Random;
import java.util.stream.Stream;

/**
 * @param <T>
 */
public class FromIdStoragePicker<T> extends Picker<T> {

    private IdStoreInterface conceptStore;
    private String typeLabel;
    private final Class<T> datatype;

    public FromIdStoragePicker(Random rand, IdStoreInterface conceptStore, String typeLabel, Class<T> datatype) {
        super(rand);
        this.conceptStore = conceptStore;
        this.typeLabel = typeLabel;
        this.datatype = datatype;
    }

    @Override
    public Stream<T> getStream(int streamLength, GraknTx tx) {
        Stream<Integer> randomUniqueOffsetStream = this.getRandomOffsetStream(streamLength, tx);
        if (randomUniqueOffsetStream == null ) {
            return Stream.empty();
        }
        return randomUniqueOffsetStream.map(randomOffset -> this.conceptStore.get(this.typeLabel, this.datatype, randomOffset));
    }

    public Integer getConceptCount(GraknTx tx) {
        return this.conceptStore.getConceptCount(this.typeLabel);
    }
}
