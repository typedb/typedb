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

package grakn.core.graph;

import grakn.core.common.exception.GraknException;
import grakn.core.graph.traversal.Traversal;
import grakn.core.graph.util.KeyGenerator;
import grakn.core.graph.util.Storage;

public class Graphs {

    private final Storage storage;
    private final KeyGenerator keyGenerator;
    private final SchemaGraph schemaGraph;
    private final DataGraph dataGraph;

    public Graphs(Storage storage) {
        this.storage = storage;
        keyGenerator = new KeyGenerator.Buffered();
        schemaGraph = new SchemaGraph(this);
        dataGraph = new DataGraph(this);
    }

    public Storage storage() {
        return storage;
    }

    public KeyGenerator keyGenerator() {
        return keyGenerator;
    }

    public SchemaGraph schema() {
        return schemaGraph;
    }

    public DataGraph data() {
        return dataGraph;
    }

    public Traversal traversal() {
        return new Traversal(); // TODO
    }

    public void clear() {
        schemaGraph.clear();
        dataGraph.clear();
    }

    public boolean isInitialised() {
        return schemaGraph.isInitialised();
    }

    public void initialise() {
        schemaGraph.initialise();
    }

    public GraknException exception(String message) {
        return storage.exception(message);
    }
}
