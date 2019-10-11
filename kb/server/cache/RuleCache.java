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

package grakn.core.kb.server.cache;

import com.google.common.annotations.VisibleForTesting;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.server.Transaction;

import java.util.Set;
import java.util.stream.Stream;

/**
 * An interface that within reasoner is cast down to the implementation to get access to a few missing methods
 * This helps break circular dependencies but needs some work
 */
public interface RuleCache {
    void setTx(Transaction tx);

    /**
     * @return set of inference rules contained in the graph
     */
    Stream<Rule> getRules();

    /**
     * @param type rule head's type
     * @param rule to be appended
     */
    void updateRules(Type type, Rule rule);

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    @VisibleForTesting
    Stream<Rule> getRulesWithType(Type type);

    /**
     * @param types to check
     * @return true if any of the provided types is absent - doesn't have instances
     */
    boolean absentTypes(Set<Type> types);

    /**
     * acknowledge addition of an instance of a specific type
     * @param type to be acked
     */
    void ackTypeInstance(Type type);

    /**
     * @param type   for which rules containing it in the head are sought
     * @param direct way of assessing isa edges
     * @return rules containing specified type in the head
     */
    Stream<Rule> getRulesWithType(Type type, boolean direct);

    /**
     * cleans cache contents
     */
    void clear();
}
