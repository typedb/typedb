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
 */

package grakn.core.graql.analytics;

import grakn.core.kb.graql.exception.GraqlQueryException;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static grakn.core.graql.analytics.KCoreVertexProgram.IMPLICIT_MESSAGE_COUNT;
import static grakn.core.graql.analytics.KCoreVertexProgram.K;
import static grakn.core.graql.analytics.KCoreVertexProgram.K_CORE_EXIST;
import static grakn.core.graql.analytics.KCoreVertexProgram.K_CORE_LABEL;
import static grakn.core.graql.analytics.KCoreVertexProgram.K_CORE_STABLE;
import static grakn.core.graql.analytics.KCoreVertexProgram.MESSAGE_COUNT;
import static grakn.core.graql.analytics.KCoreVertexProgram.atRelations;
import static grakn.core.graql.analytics.KCoreVertexProgram.filterByDegree;
import static grakn.core.graql.analytics.KCoreVertexProgram.relayOrSaveMessages;
import static grakn.core.graql.analytics.KCoreVertexProgram.sendMessage;
import static grakn.core.graql.analytics.KCoreVertexProgram.updateEntityAndAttribute;

/**
 * The vertex program for computing coreness using k-core.
 * <p>
 * https://en.wikipedia.org/wiki/Degeneracy_(graph_theory)#k-Cores
 * </p>
 *
 */

public class CorenessVertexProgram extends GraknVertexProgram<String> {

    private static final int MAX_ITERATION = 200;
    private static final String EMPTY_MESSAGE = "";

    public static final String CORENESS = "corenessVertexProgram.coreness";

    private static final String PERSIST_CORENESS = "corenessVertexProgram.persistCoreness";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = newHashSet(
            MemoryComputeKey.of(K_CORE_STABLE, Operator.and, false, true),
            MemoryComputeKey.of(K_CORE_EXIST, Operator.or, false, true),
            MemoryComputeKey.of(PERSIST_CORENESS, Operator.assign, true, true),
            MemoryComputeKey.of(K, Operator.assign, true, true));

    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = newHashSet(
            VertexComputeKey.of(CORENESS, false),
            VertexComputeKey.of(K_CORE_LABEL, true),
            VertexComputeKey.of(MESSAGE_COUNT, true),
            VertexComputeKey.of(IMPLICIT_MESSAGE_COUNT, true));

    @SuppressWarnings("unused")// Needed internally for OLAP tasks
    public CorenessVertexProgram() {
    }

    public CorenessVertexProgram(long minK) {
        this.persistentProperties.put(K, minK);
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
        memory.set(K, persistentProperties.get(K));
    }

    @Override
    public void safeExecute(final Vertex vertex, Messenger<String> messenger, final Memory memory) {
        switch (memory.getIteration()) {
            case 0:
                sendMessage(messenger, EMPTY_MESSAGE);
                break;

            case 1: // get degree first, as degree must >= k
                filterByDegree(vertex, messenger, memory, false);
                break;

            default:
                if (memory.<Boolean>get(PERSIST_CORENESS) && vertex.property(K_CORE_LABEL).isPresent()) {
                    // persist coreness
                    vertex.property(CORENESS, memory.<Long>get(K) - 1L);

                    // check if the vertex should included for the next k value
                    if (vertex.<Long>value(MESSAGE_COUNT) < memory.<Long>get(K)) {
                        vertex.property(K_CORE_LABEL).remove();
                        break;
                    }
                }

                // relay message through relation vertices in even iterations
                // send message from regular entities in odd iterations
                if (atRelations(memory)) {
                    relayOrSaveMessages(vertex, messenger);
                } else {
                    updateEntityAndAttribute(vertex, messenger, memory, true);
                }
                break;
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        if (memory.isInitialIteration()) {
            LOGGER.debug("Finished Iteration {}", memory.getIteration());
            return false;
        }

        if (memory.getIteration() == MAX_ITERATION) {
            LOGGER.debug("Reached Max Iteration: {}", MAX_ITERATION);
            throw GraqlQueryException.maxIterationsReached(this.getClass());
        }

        if (memory.<Boolean>get(PERSIST_CORENESS)) {
            memory.set(PERSIST_CORENESS, false);
        }
        if (!atRelations(memory)) {
            LOGGER.debug("UpdateEntityAndAttribute... Finished Iteration {}", memory.getIteration());
            if (!memory.<Boolean>get(K_CORE_EXIST)) {
                LOGGER.debug("KCoreVertexProgram Finished");
                return true;
            } else {
                if (memory.<Boolean>get(K_CORE_STABLE)) {
                    LOGGER.debug("Found Core Areas K = {}\n", memory.<Long>get(K));
                    memory.set(K, memory.<Long>get(K) + 1L);
                    memory.set(PERSIST_CORENESS, true);
                } else {
                    memory.set(K_CORE_STABLE, true);
                }
                memory.set(K_CORE_EXIST, false);
                return false;
            }
        } else {
            LOGGER.debug("RelayOrSaveMessage...       Finished Iteration {}", memory.getIteration());
            return false; // can not end after relayOrSaveMessages
        }
    }
}