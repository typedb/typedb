/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.Objects;

public class GraphManager {

    private final TypeGraph typeGraph;

    private final ThingGraph thingGraph;

    public GraphManager(TypeGraph typeGraph, ThingGraph thingGraph) {
        this.typeGraph = typeGraph;
        this.thingGraph = thingGraph;
    }

    public TypeGraph schema() {
        return typeGraph;
    }

    public ThingGraph data() {
        return thingGraph;
    }

    public void clear() {
        typeGraph.clear();
        thingGraph.clear();
    }

    public TypeDBException exception(ErrorMessage error) {
        return thingGraph.storage().exception(error);
    }

    public TypeDBException exception(Exception exception) {
        return thingGraph.storage().exception(exception);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraphManager graphMgr = (GraphManager) o;
        return typeGraph.equals(graphMgr.typeGraph) && thingGraph.equals(graphMgr.thingGraph);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeGraph, thingGraph);
    }
}
