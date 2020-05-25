/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package hypergraph.graph;

import hypergraph.graph.util.KeyGenerator;
import hypergraph.graph.util.Storage;

public class Graphs {

    private final Storage storage;
    private final KeyGenerator keyGenerator;
    private final TypeGraph typeGraph;
    private final ThingGraph thingGraph;

    public Graphs(Storage storage) {
        this.storage = storage;
        keyGenerator = new KeyGenerator.Buffered();
        typeGraph = new TypeGraph(this);
        thingGraph = new ThingGraph(this);
    }

    public Storage storage() {
        return storage;
    }

    public KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    public TypeGraph type() {
        return typeGraph;
    }

    public ThingGraph thing() {
        return thingGraph;
    }

    public void clear() {
        typeGraph.clear();
        thingGraph.clear();
    }

    public boolean isInitialised() {
        return typeGraph.isInitialised();
    }

    public void initialise() {
        typeGraph.initialise();
    }

    /**
     * Commits any writes captured in the graphs into storage.
     *
     * This operation may result in locking the storage to confirm that it gets
     * committed. If it is locking, you must call {@code confirm(boolean committed)}
     * to confirm whether the graph was successfully committed or not into storage.
     *
     * @return true if the operation results in locking the storage
     */
    public boolean commit() {
        return typeGraph.commit() || thingGraph.commit();
    }

    /**
     * Confirms whether the graph was successfully committed into storage.
     *
     * This operation will release any lock on the storage, that the
     * {@code commit()} operation may have acquired.
     *
     * @param committed whether the graph was successfully committed into storage
     * @param snapshot  sequence number of storage if graph was successfully committed
     */
    public void confirm(boolean committed, long snapshot) {
        thingGraph.confirm(committed, snapshot);
    }
}
