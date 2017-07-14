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

import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * A vertex program specific to Grakn with common method implementations.
 * <p>
 *
 * @param <T> the type of messages being sent between vertices
 * @author Jason Liu
 * @author Sheldon Hall
 */

public abstract class GraknVertexProgram<T> extends CommonOLAP implements VertexProgram<T> {

    static final Logger LOGGER = LoggerFactory.getLogger(GraknVertexProgram.class);

    static final MessageScope.Local<?> messageScopeShortcutIn = MessageScope.Local.of(() -> __.inE(
            Schema.EdgeLabel.SHORTCUT.getLabel()));
    static final MessageScope.Local<?> messageScopeShortcutOut = MessageScope.Local.of(() -> __.outE(
            Schema.EdgeLabel.SHORTCUT.getLabel()));
    static final MessageScope.Local<?> messageScopeResourceIn = MessageScope.Local.of(() -> __.inE(
            Schema.EdgeLabel.RESOURCE.getLabel()));
    static final MessageScope.Local<?> messageScopeResourceOut = MessageScope.Local.of(() -> __.outE(
            Schema.EdgeLabel.RESOURCE.getLabel()));
    static final Set<MessageScope> messageScopeSetShortcut =
            Sets.newHashSet(messageScopeShortcutIn, messageScopeShortcutOut,
                    messageScopeResourceIn, messageScopeResourceOut);

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return messageScopeSetShortcut;
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);

        // store class name for reflection on spark executor
        configuration.setProperty(VERTEX_PROGRAM, this.getClass().getName());
    }

    @Override
    public void setup(final Memory memory) {
    }

    @Override
    public void execute(Vertex vertex, Messenger<T> messenger, Memory memory) {
        // try to deal with ghost vertex issues by ignoring them
        if (Utility.isAlive(vertex)) {
            safeExecute(vertex, messenger, memory);
        }
    }

    /**
     * An alternative to the execute method when ghost vertices are an issue. Our "Ghostbuster".
     *
     * @param vertex    a vertex that may be a ghost
     * @param messenger Tinker message passing object
     * @param memory    Tinker memory object
     */
    abstract void safeExecute(Vertex vertex, Messenger<T> messenger, Memory memory);

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    // super.clone() will always return something of the correct type
    @SuppressWarnings("unchecked")
    @Override
    public GraknVertexProgram<T> clone() {
        try {
            return (GraknVertexProgram) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw CommonUtil.unreachableStatement(e);
        }
    }

    static long getMessageCount(Messenger<Long> messenger) {
        return IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
    }
}
