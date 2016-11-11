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
 */

package ai.grakn.graql.internal.analytics;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is not used in analytics, as CountMapReduce is consistently faster.
 */

public class CountVertexProgram extends GraknVertexProgram {

    public static final String COUNT = "countVertexProgram.count";

    private static final Set<String> MEMORY_COMPUTE_KEYS = new HashSet<>(Collections.singletonList(COUNT));

    public CountVertexProgram() {
    }

    public CountVertexProgram(Set<String> types) {
        selectedTypes = types;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.emptySet();
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(COUNT, 0L);
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger messenger, final Memory memory) {
        if (selectedTypes.contains(Utility.getVertexType(vertex))) {
            memory.incr(COUNT, 1L);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 0;
    }

}
