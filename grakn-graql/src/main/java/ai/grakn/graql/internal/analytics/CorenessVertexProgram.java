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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.analytics;

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.util.Schema;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.IMPLICIT_MESSAGE_COUNT;
import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.K_CORE_EXIST;
import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.K_CORE_STABLE;
import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.checkDegree;
import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.getMessageCountExcludeSelf;
import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.relayOrSaveMessages;
import static ai.grakn.graql.internal.analytics.KCoreVertexProgram.sendMessage;
import static com.google.common.collect.Sets.newHashSet;

/**
 * The vertex program for computing coreness using k-core.
 *
 * @author Jason Liu
 */

public class CorenessVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 200;
    private static final String EMPTY_MESSAGE = "";

    public static final String CORENESS = "corenessVertexProgram.coreness";

    private static final String MESSAGE_COUNT = "corenessVertexProgram.messageCount";
    private static final String QUALIFIED = "corenessVertexProgram.qualified";
    private static final String PERSIST_CORENESS = "corenessVertexProgram.persistCoreness";
    private static final String MIN_K = "corenessVertexProgram.k";
    private static final String CURRENT_K = "corenessVertexProgram.currentK";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = newHashSet(
            MemoryComputeKey.of(K_CORE_STABLE, Operator.and, false, true),
            MemoryComputeKey.of(K_CORE_EXIST, Operator.or, false, true),
            MemoryComputeKey.of(PERSIST_CORENESS, Operator.assign, true, true),
            MemoryComputeKey.of(CURRENT_K, Operator.assign, true, true));

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = newHashSet(
            VertexComputeKey.of(CORENESS, false),
            VertexComputeKey.of(QUALIFIED, true),
            VertexComputeKey.of(MESSAGE_COUNT, true),
            VertexComputeKey.of(IMPLICIT_MESSAGE_COUNT, true));

    private int minK;

    // Needed internally for OLAP tasks
    public CorenessVertexProgram() {
    }

    public CorenessVertexProgram(int minK) {
        this.minK = minK;
        this.persistentProperties.put(MIN_K, minK);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        persistentProperties.put(MIN_K, minK);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        super.loadState(graph, configuration);
        minK = (int) persistentProperties.get(MIN_K);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        LOGGER.debug("KCoreVertexProgram Started !!!!!!!!");

        // K_CORE_STABLE is true by default, and we reset it after each odd iteration.
        memory.set(K_CORE_STABLE, false);
        memory.set(K_CORE_EXIST, false);
        memory.set(PERSIST_CORENESS, false);
        memory.set(CURRENT_K, persistentProperties.get(MIN_K));
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                sendMessage(messenger, EMPTY_MESSAGE);
                break;

            case 1: // get degree first, as degree must >= k
                checkDegree(vertex, messenger, memory, memory.<Integer>get(CURRENT_K), QUALIFIED);
                break;

            default:
                if (memory.<Boolean>get(PERSIST_CORENESS) && vertex.property(QUALIFIED).isPresent()) {
                    // persist coreness
                    vertex.property(CORENESS, memory.<Integer>get(CURRENT_K) - 1);

                    // check if the vertex should included for the next k value
                    if ((int) vertex.value(MESSAGE_COUNT) < memory.<Integer>get(CURRENT_K)) {
                        vertex.property(QUALIFIED).remove();
                        break;
                    }
                }

                // relay message through relationship vertices in even iterations
                // send message from regular entities in odd iterations
                if (memory.getIteration() % 2 == 0) {
                    relayOrSaveMessages(vertex, messenger, QUALIFIED);
                } else {
                    updateEntityAndAttribute(vertex, messenger, memory);
                }
                break;
        }
    }

    private void updateEntityAndAttribute(Vertex vertex, Messenger<String> messenger, Memory memory) {
        if (vertex.property(QUALIFIED).isPresent()) {
            String id = vertex.value(Schema.VertexProperty.ID.name());
            int messageCount = getMessageCountExcludeSelf(messenger, id) +
                    (vertex.property(IMPLICIT_MESSAGE_COUNT).isPresent() ?
                            (int) vertex.value(IMPLICIT_MESSAGE_COUNT) : 0);

            if (messageCount >= memory.<Integer>get(CURRENT_K)) {
                LOGGER.trace("Sending msg from " + id);
                sendMessage(messenger, id);
                memory.add(K_CORE_EXIST, true);

                // message count may help eliminate unqualified vertex in earlier iterations
                vertex.property(MESSAGE_COUNT, messageCount);
            } else {
                LOGGER.trace("Removing label of " + id);
                vertex.property(QUALIFIED).remove();
                memory.add(K_CORE_STABLE, false);
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        if (memory.isInitialIteration()) {
            LOGGER.debug("Finished Iteration " + memory.getIteration());
            return false;
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: " + MAX_ITERATION + " !!!!!!!!");
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        if (memory.<Boolean>get(PERSIST_CORENESS)) {
            memory.set(PERSIST_CORENESS, false);
        }
        if (memory.getIteration() % 2 != 0) {
            LOGGER.debug("UpdateEntityAndAttribute... Finished Iteration " + memory.getIteration());
            if (!memory.<Boolean>get(K_CORE_EXIST)) {
                LOGGER.debug("KCoreVertexProgram Finished !!!!!!!!");
                return true;
            } else {
                if (memory.<Boolean>get(K_CORE_STABLE)) {
                    LOGGER.debug("Found Core Areas K = " + memory.<Integer>get(CURRENT_K) + "\n");
                    memory.set(CURRENT_K, memory.<Integer>get(CURRENT_K) + 1);
                    memory.set(PERSIST_CORENESS, true);
                } else {
                    memory.set(K_CORE_STABLE, true);
                }
                memory.set(K_CORE_EXIST, false);
                return false;
            }
        } else {
            LOGGER.debug("RelayOrSaveMessage...       Finished Iteration " + memory.getIteration());
            return false; // can not end after relayOrSaveMessages
        }
    }
}