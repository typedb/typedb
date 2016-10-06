/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.util.ErrorMessage;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;

/**
 * A vertex program specific to Mindmaps with common method implementations.
 */
public abstract class MindmapsVertexProgram<T> extends CommonOLAP implements VertexProgram<T> {

    final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
    final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(countMessageScopeOut);
        set.add(countMessageScopeIn);
        return set;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);

        // store class name for reflection on spark executor
        configuration.setProperty(VERTEX_PROGRAM, this.getClass().getName());
    }

    @Override
    public void setup(final Memory memory) {}

    @Override
    public void execute(Vertex vertex, Messenger<T> messenger, Memory memory) {
        // try to deal with ghost vertex issues by ignoring them
        if (isAlive(vertex)) {
            safeExecute(vertex, messenger, memory);
        }
    }

    /**
     * An alternative to the execute method when ghost vertices are an issue. Our "Ghostbuster".
     *
     * @param vertex        a vertex that may be a ghost
     * @param messenger     Tinker message passing object
     * @param memory        Tinker memory object
     */
    abstract void safeExecute(Vertex vertex, Messenger<T> messenger, Memory memory);

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public MindmapsVertexProgram clone() {
        try {
            final MindmapsVertexProgram clone = (MindmapsVertexProgram) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(),e.getMessage()),e);
        }
    }

}
