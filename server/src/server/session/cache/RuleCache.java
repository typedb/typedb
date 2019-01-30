/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.session.cache;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.server.session.TransactionOLTP;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Caches rules applicable to schema concepts and their conversion to InferenceRule object (parsing is expensive when large number of rules present).
 * NB: non-committed rules are also cached.
 */
public class RuleCache {

    private final HashMultimap<Type, Rule> ruleMap = HashMultimap.create();
    private final Map<Rule, Object> ruleConversionMap = new HashMap<>();
    private final TransactionOLTP tx;

    public RuleCache(TransactionOLTP tx) {
        this.tx = tx;
    }

    /**
     * @return set of inference rules contained in the graph
     */
    public Stream<Rule> getRules() {
        Rule metaRule = tx.getMetaRule();
        return metaRule.subs().filter(sub -> !sub.equals(metaRule));
    }

    /**
     * @param type rule head's type
     * @param rule to be appended
     * @return updated entry value
     */
    public Set<Rule> updateRules(Type type, Rule rule) {
        Set<Rule> match = ruleMap.get(type);
        if (match == null) {
            Set<Rule> rules = Sets.newHashSet(rule);
            getTypes(type, false).stream()
                    .flatMap(SchemaConcept::thenRules)
                    .forEach(r -> ruleMap.put(type, r));
            return rules;
        }
        ruleMap.put(type, rule);
        match.add(rule);
        return match;
    }

    /**
     *
     * @param type of interest
     * @param direct true if type hierarchy shouldn't be included
     * @return relevant part (direct only or subs) of the type hierarchy of a type
     */
    private Set<Type> getTypes(Type type, boolean direct) {
        Set<Type> types = direct ? Sets.newHashSet(type) : type.subs().collect(Collectors.toSet());
        return type.isImplicit() ?
                types.stream().flatMap(t -> Stream.of(t, tx.getType(Schema.ImplicitType.explicitLabel(t.label())))).collect(Collectors.toSet()) :
                types;
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    public Stream<Rule> getRulesWithType(Type type) {
        return getRulesWithType(type, false);
    }

    /**
     * @param type   for which rules containing it in the head are sought
     * @param direct way of assessing isa edges
     * @return rules containing specified type in the head
     */
    public Stream<Rule> getRulesWithType(Type type, boolean direct) {
        if (type == null) return getRules();

        Set<Rule> match = ruleMap.get(type);
        if (match != null) return match.stream();

        return getTypes(type, direct).stream()
                .flatMap(SchemaConcept::thenRules)
                .peek(rule -> ruleMap.put(type, rule));
    }

    /**
     * @param rule      for which the parsed rule should be retrieved
     * @param converter rule converter
     * @param <T>       type of object converter converts to
     * @return parsed rule object
     */
    public <T> T getRule(Rule rule, Supplier<T> converter) {
        T match = (T) ruleConversionMap.get(rule);
        if (match != null) return match;

        T newMatch = converter.get();
        ruleConversionMap.put(rule, newMatch);
        return newMatch;
    }

    /**
     * cleans cache contents
     */
    public void closeTx() {
        ruleMap.clear();
        ruleConversionMap.clear();
    }
}
