/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.core.log;

import grakn.core.graph.core.JanusGraphTransaction;

/**
 * Allows the user to define custom behavior to process those transactional changes that are recorded in a transaction LOG.
 * {@link ChangeProcessor}s are registered with a transaction LOG processor in the {@link LogProcessorBuilder}.
 */
public interface ChangeProcessor {

    /**
     * Process the changes caused by the transaction identified by {@code txId} within a newly opened transaction {@code tx}.
     * The changes are captured in the {@link ChangeState} data structure.
     */
    void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState);

}
