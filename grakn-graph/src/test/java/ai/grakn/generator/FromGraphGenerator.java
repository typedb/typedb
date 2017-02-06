/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.generator;

import ai.grakn.GraknGraph;

import static ai.grakn.generator.GraknGraphs.lastGeneratedGraph;

public abstract class FromGraphGenerator<T> extends AbstractGenerator<T> {
    FromGraphGenerator(Class<T> type) {
        super(type);
    }

    private boolean useExistingGraph = false;

    protected final GraknGraph graph() {
        if (useExistingGraph) {
            return lastGeneratedGraph();
        } else {
            return gen(GraknGraph.class);
        }
    }

    public final void configure(FromGraph fromGraph) {
        useExistingGraph = true;
    }
}
