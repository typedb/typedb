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
import storage.IdStoreInterface;

import java.util.Random;
import java.util.stream.Stream;

/**
 *
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
