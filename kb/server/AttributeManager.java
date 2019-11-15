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
 *
 */


package grakn.core.kb.server;

import com.google.common.cache.Cache;
import grakn.core.kb.concept.api.ConceptId;

public interface AttributeManager {

    Cache<String, ConceptId> attributesCache();

    void ackAttributeInsert(String index, String txId);
    void ackAttributeDelete(String index, String txId);
    void ackAttributeCommit(String index, String txId);
    void ackCommit(String txId);

    boolean requiresLock(String txId);

    void printEphemeralCache();

}
