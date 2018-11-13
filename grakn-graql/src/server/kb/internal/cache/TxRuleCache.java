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

package grakn.core.server.kb.internal.cache;

import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.server.kb.internal.TransactionImpl;
import grakn.core.graql.internal.Schema;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Caches rules applicable to schema concepts and their conversion to InferenceRule object (parsing is expensive when large number of rules present).
 * NB: non-committed rules are alse cached.
 *
 */
public class TxRuleCache {

    private final Map<SchemaConcept, Set<Rule>> ruleMap = new HashMap<>();
    private final Map<Rule, Object> ruleConversionMap = new HashMap<>();
    private final TransactionImpl tx;

    public TxRuleCache(TransactionImpl tx){
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
     * @param type to be update
     * @param rule to be appended
     * @return updated entry vlue
     */
    public Set<Rule> updateRules(SchemaConcept type, Rule rule){
        Set<Rule> match = ruleMap.get(type);
        if (match == null){
            Set<Rule> rules = Sets.newHashSet(rule);
            getTypes(type, false).stream()
                    .flatMap(SchemaConcept::thenRules)
                    .forEach(rules::add);
            ruleMap.put(type, rules);
            return rules;
        }
        match.add(rule);
        return match;
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    public Stream<Rule> getRulesWithType(SchemaConcept type){
        return getRulesWithType(type, false);
    }

    private Set<SchemaConcept> getTypes(SchemaConcept type, boolean direct) {
        Set<SchemaConcept> types = direct ? Sets.newHashSet(type) : type.subs().collect(Collectors.toSet());
        return type.isImplicit()?
                types.stream().flatMap(t -> Stream.of(t, tx.getSchemaConcept(Schema.ImplicitType.explicitLabel(t.label())))).collect(Collectors.toSet()):
                types;
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @param direct way of assessing isa edges
     * @return rules containing specified type in the head
     */
    public Stream<Rule> getRulesWithType(SchemaConcept type, boolean direct){
        if (type == null) return getRules();

        Set<Rule> match = ruleMap.get(type);
        if (match != null) return match.stream();

        Set<Rule> rules = new HashSet<>();
        ruleMap.put(type, rules);
        return getTypes(type, direct).stream()
                .flatMap(SchemaConcept::thenRules)
                .peek(rules::add);
    }

    /**
     *
     * @param rule for which the parsed rule should be retrieved
     * @param converter rule converter
     * @param <T> type of object converter converts to
     * @return parsed rule object
     */
    public <T> T getRule(Rule rule, Supplier<T> converter){
        T match = (T) ruleConversionMap.get(rule);
        if (match != null) return match;

        T newMatch = converter.get();
        ruleConversionMap.put(rule, newMatch);
        return newMatch;
    }

    /**
     * cleans cache contents
     */
    public void closeTx(){
        ruleMap.clear();
        ruleConversionMap.clear();
    }
}
