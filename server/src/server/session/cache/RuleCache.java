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
import grakn.core.concept.type.Rule;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.TransactionOLTP;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * Caches rules applicable to schema concepts and their conversion to InferenceRule object (parsing is expensive when large number of rules present).
 * NB: non-committed rules are also cached.
 */
public class RuleCache {

    private final HashMultimap<Type, Rule> ruleMap = HashMultimap.create();
    private final Map<Rule, Object> ruleConversionMap = new HashMap<>();
    private final TransactionOLTP tx;

    //TODO: these should be eventually stored together with statistics
    private Set<Type> absentTypes = new HashSet<>();
    private Set<Type> checkedTypes = new HashSet<>();
    private Set<Rule> fruitlessRules = new HashSet<>();
    private Set<Rule> checkedRules = new HashSet<>();

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
     */
    public void updateRules(Type type, Rule rule) {
        Set<Rule> match = ruleMap.get(type);
        if (match.isEmpty()) {
            getTypes(type, false).stream()
                    .flatMap(SchemaConcept::thenRules)
                    .forEach(r -> ruleMap.put(type, r));
        }
        ruleMap.put(type, rule);
    }

    /**
     *
     * @param type of interest
     * @param direct true if type hierarchy shouldn't be included
     * @return relevant part (direct only or subs) of the type hierarchy of a type
     */
    private Set<Type> getTypes(Type type, boolean direct) {
        Set<Type> types = direct ? Sets.newHashSet(type) : type.subs().collect(toSet());
        return type.isImplicit() ?
                types.stream().flatMap(t -> Stream.of(t, tx.getType(Schema.ImplicitType.explicitLabel(t.label())))).collect(toSet()) :
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
     * @param types to check
     * @return true if any of the provided types is absent - doesn't have instances
     */
    public boolean absentTypes(Set<Type> types) {
        return types.stream().anyMatch(t -> !typeHasInstances(t));
    }

    /**
     * acknowledge addition of an instance of a specific type
     * @param type to be acked
     */
    public void ackTypeInstance(Type type){
        checkedTypes.add(type);
        absentTypes.remove(type);
    }

    /**
     * @param type   for which rules containing it in the head are sought
     * @param direct way of assessing isa edges
     * @return rules containing specified type in the head
     */
    public Stream<Rule> getRulesWithType(Type type, boolean direct) {
        if (type == null) return getRules();

        Set<Rule> match = ruleMap.get(type);
        if (!match.isEmpty()) return match.stream();

        return getTypes(type, direct).stream()
                .flatMap(SchemaConcept::thenRules)
                .filter(this::checkRule)
                .peek(rule -> ruleMap.put(type, rule));
    }

    private boolean typeHasInstances(Type type){
        if (checkedTypes.contains(type)) return !absentTypes.contains(type);
        checkedTypes.add(type);
        boolean instancePresent = type.instances().findFirst().isPresent()
                || type.thenRules().anyMatch(this::checkRule);
        if (!instancePresent){
            absentTypes.add(type);
            type.whenRules().forEach(r -> fruitlessRules.add(r));
        }
        return instancePresent;
    }

    /**
     *
     * @param rule to be checked for matchability
     * @return true if rule is matchable (can provide answers)
     */
    private boolean checkRule(Rule rule){
        if (fruitlessRules.contains(rule)) return false;
        if (checkedRules.contains(rule)) return true;
        checkedRules.add(rule);
        return rule.whenTypes()
                //.filter(t -> !checkedTypes.contains(t))
                .allMatch(this::typeHasInstances);
                //.peek(t ->
                //.findFirst().isPresent();
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
    public void clear() {
        ruleMap.clear();
        ruleConversionMap.clear();
        absentTypes.clear();
        checkedTypes.clear();
        checkedRules.clear();
        fruitlessRules.clear();
    }
}
