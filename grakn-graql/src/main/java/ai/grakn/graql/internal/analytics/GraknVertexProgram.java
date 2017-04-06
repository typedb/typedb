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

import ai.grakn.util.ErrorMessage;
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

import java.util.Iterator;
import java.util.Set;

/**
 * A vertex program specific to Grakn with common method implementations.
 * <p>
 *
 * @param <T> the type of messages being sent between vertices
 *
 * @author Jason Liu
 * @author Sheldon Hall
 */

public abstract class GraknVertexProgram<T> extends CommonOLAP implements VertexProgram<T> {

    static final Logger LOGGER = LoggerFactory.getLogger(GraknVertexProgram.class);

    static final MessageScope.Local<?> messageScopeIn = MessageScope.Local.of(() -> __.<Vertex>inE(
            Schema.EdgeLabel.CASTING.getLabel(),
            Schema.EdgeLabel.ROLE_PLAYER.getLabel()));
    static final MessageScope.Local<?> messageScopeOut = MessageScope.Local.of(() -> __.<Vertex>outE(
            Schema.EdgeLabel.CASTING.getLabel(),
            Schema.EdgeLabel.ROLE_PLAYER.getLabel()));
    private static final Set<MessageScope> messageScopeSet = Sets.newHashSet(messageScopeIn, messageScopeOut);

    static final MessageScope.Local<?> messageScopeInCasting = MessageScope.Local.of(() -> __.<Vertex>inE(
            Schema.EdgeLabel.CASTING.getLabel()));
    static final MessageScope.Local<?> messageScopeOutCasting = MessageScope.Local.of(() -> __.<Vertex>outE(
            Schema.EdgeLabel.CASTING.getLabel()));
    static final MessageScope.Local<?> messageScopeInRolePlayer = MessageScope.Local.of(() -> __.<Vertex>inE(
            Schema.EdgeLabel.ROLE_PLAYER.getLabel()));
    static final MessageScope.Local<?> messageScopeOutRolePlayer = MessageScope.Local.of(() -> __.<Vertex>outE(
            Schema.EdgeLabel.ROLE_PLAYER.getLabel()));
    static final Set<MessageScope> messageScopeSetInstance =
            Sets.newHashSet(messageScopeInRolePlayer, messageScopeOutCasting);
    static final Set<MessageScope> messageScopeSetCasting =
            Sets.newHashSet(messageScopeInCasting, messageScopeOutRolePlayer);

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return messageScopeSet;
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
            throw new IllegalStateException(
                    ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(), e.getMessage()), e);
        }
    }

    void degreeStepInstance(Vertex vertex, Messenger<Long> messenger) {
        String type = vertex.label();
        if (type.equals(Schema.BaseType.ENTITY.name()) || type.equals(Schema.BaseType.RESOURCE.name())) {
            // each role-player sends 1 to castings following incoming edges
            messenger.sendMessage(messageScopeInRolePlayer, 1L);
        } else if (type.equals(Schema.BaseType.RELATION.name())) {
            // the assertion can also be role-player, so sending 1 to castings following incoming edges
            messenger.sendMessage(messageScopeInRolePlayer, 1L);
            // send -1 to castings following outgoing edges
            messenger.sendMessage(messageScopeOutCasting, -1L);
        }
    }

    void degreeStepCasting(Messenger<Long> messenger) {
        boolean hasPlayer = false;
        long assertionCount = 0;
        Iterator<Long> iterator = messenger.receiveMessages();
        while (iterator.hasNext()) {
            long message = iterator.next();
            // count number of assertions connected
            if (message < 0) assertionCount++;
            else hasPlayer = true;
        }

        // make sure this role-player is in the subgraph
        if (hasPlayer) {
            messenger.sendMessage(messageScopeInCasting, 1L);
            messenger.sendMessage(messageScopeOutRolePlayer, assertionCount);
        }
    }

    static long getMessageCount(Messenger<Long> messenger) {
        return IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
    }
}
