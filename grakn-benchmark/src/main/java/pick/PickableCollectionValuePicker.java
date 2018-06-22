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
import strategy.PickableCollection;

import java.util.stream.Stream;

/**
 * @param <T>
 */
public class PickableCollectionValuePicker<T> implements StreamInterface<T> {

    private PickableCollection<T> valueOptions;

    public PickableCollectionValuePicker(PickableCollection<T> valueOptions) {
        this.valueOptions = valueOptions;
    }

    @Override
    public Stream<T> getStream(int streamLength, GraknTx tx) {
        return Stream.generate(() -> valueOptions.next()).limit(streamLength);
    }
}