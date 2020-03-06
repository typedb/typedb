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
 *
 */

package grakn.core.graql.reasoner.cache;

import com.google.common.annotations.VisibleForTesting;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.reasoner.cache.RuleCache;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.pattern.Pattern;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Caches rules applicable to schema concepts and their conversion to InferenceRule object (parsing is expensive when large number of rules present).
 * NB: non-committed rules are also cached.
 */
public class RuleCacheImpl implements RuleCache {

    //NB: we specifically use map to differentiate between type with no rules (empty set) and unchecked type (null)
    private final Map<Type, Set<Rule>> ruleMap = new HashMap<>();
    private final Map<Rule, InferenceRule> ruleConversionMap = new HashMap<>();
    private final ConceptManager conceptManager;
    private final KeyspaceStatistics keyspaceStatistics;

    private Set<Type> absentTypes = new HashSet<>();
    private Set<Type> checkedTypes = new HashSet<>();
    private Set<Rule> unmatchableRules = new HashSet<>();
    private Set<Rule> checkedRules = new HashSet<>();
    private ReasonerQueryFactory reasonerQueryFactory;

    public RuleCacheImpl(ConceptManager conceptManager, KeyspaceStatistics keyspaceStatistics) {
        this.conceptManager = conceptManager;
        this.keyspaceStatistics = keyspaceStatistics;
    }

    /*
    TODO remove this when circular deps are broken
     */
    public void setReasonerQueryFactory(ReasonerQueryFactory reasonerQueryFactory) {
        this.reasonerQueryFactory = reasonerQueryFactory;
    }

    /**
     * @return set of inference rules contained in the graph
     */
    @Override
    public Stream<Rule> getRules() {
        Rule metaRule = conceptManager.getMetaRule();
        return metaRule.subs().filter(sub -> !sub.equals(metaRule));
    }

    /**
     * @param rule to be appended
     */
    @Override
    public void ackRuleInsertion(Rule rule) {
        Pattern thenPattern = rule.then();
        if (thenPattern == null) return;
        //NB: thenTypes() will be empty as type edges added on commit
        //NB: this will cache also non-committed rules
        thenPattern.statements().stream()
                .flatMap(v -> v.getTypes().stream())
                .map(type -> conceptManager.<SchemaConcept>getSchemaConcept(Label.of(type)))
                .filter(Objects::nonNull)
                .filter(Concept::isType)
                .map(Concept::asType)
                .forEach(type -> {
                    Set<Rule> match = ruleMap.get(type);
                    if (match == null) {
                        Set<Rule> rules = new HashSet<>();
                        rules.add(rule);
                        getTypes(type, false)
                                .flatMap(SchemaConcept::thenRules)
                                .forEach(rules::add);
                        ruleMap.put(type, rules);
                    } else {
                        match.add(rule);
                    }
                });
    }

    /**
     *
     * @param type of interest
     * @param direct true if type hierarchy shouldn't be included
     * @return relevant part (direct only or subs) of the type hierarchy of a type
     */
    private Stream<? extends Type> getTypes(Type type, boolean direct) {
        Stream<? extends Type> baseStream = direct ? Stream.of(type) : type.subs();
        if (type.isImplicit()) {
            return baseStream
                    .flatMap(t -> Stream.of(t, conceptManager.getType(Schema.ImplicitType.explicitLabel(t.label()))))
                    .filter(Objects::nonNull);
        }
        return baseStream;
    }

    /**
     * @param type for which rules containing it in the head are sought
     * @return rules containing specified type in the head
     */
    @Override
    @VisibleForTesting
    public Stream<Rule> getRulesWithType(Type type) {
        return getRulesWithType(type, false);
    }

    /**
     * @param types to check
     * @return true if any of the provided types is absent - doesn't have instances
     */
    @Override
    public boolean absentTypes(Set<Type> types) {
        return types.stream().anyMatch(t -> !typeHasInstances(t));
    }

    /**
     * acknowledge addition of an instance of a specific type
     * @param type to be acked
     */
    @Override
    public void ackTypeInstanceInsertion(Type type){
        checkedTypes.add(type);
        absentTypes.remove(type);
    }

    /**
     * @param type   for which rules containing it in the head are sought
     * @param direct way of assessing isa edges
     * @return rules containing specified type in the head
     */
    @Override
    public Stream<Rule> getRulesWithType(Type type, boolean direct) {
        if (type == null) return getRules();

        Set<Rule> match = ruleMap.get(type);
        if (match != null) return match.stream();

        Set<Rule> rules = new HashSet<>();
        getTypes(type, direct)
                .flatMap(SchemaConcept::thenRules)
                .filter(this::isRuleMatchable)
                .forEach(rules::add);
        ruleMap.put(type, rules);

        return rules.stream();
    }

    private boolean instancePresent(Type type){
        boolean instanceCountPresent = type.subs()
                .anyMatch(t -> keyspaceStatistics.count(conceptManager, t.label()) != 0);
        if (instanceCountPresent) return true;

        //NB: this is a defensive check, it stat count shows 0 (no instances) we additionally check the DB
        //also, if we have ephemeral instances (inserted but not committed), we will catch them by doing the DB check
        return type.instances().findFirst().isPresent();
    }

    private boolean typeHasInstances(Type type){
        if (checkedTypes.contains(type)) return !absentTypes.contains(type);
        checkedTypes.add(type);
        boolean instancePresent =instancePresent(type)
                || type.subs().flatMap(SchemaConcept::thenRules).anyMatch(this::isRuleMatchable);
        if (!instancePresent){
            absentTypes.add(type);
            type.whenRules()
                    .filter(rule -> rule.whenPositiveTypes().anyMatch(pt -> pt.equals(type)))
                    .forEach(r -> unmatchableRules.add(r));
        }
        return instancePresent;
    }

    /**
     *
     * @param rule to be checked for matchability
     * @return true if rule is matchable (can provide answers)
     */
    private boolean isRuleMatchable(Rule rule){
        if (unmatchableRules.contains(rule)) return false;
        if (checkedRules.contains(rule)) return true;
        checkedRules.add(rule);
        return rule.whenPositiveTypes()
                .allMatch(this::typeHasInstances);
    }

    /**
     * @param rule      for which the parsed rule should be retrieved
     * @return parsed rule object
     */
    public InferenceRule getRule(Rule rule) {
        InferenceRule match = ruleConversionMap.get(rule);
        if (match != null) return match;

        InferenceRule newMatch = new InferenceRule(rule, reasonerQueryFactory);
        ruleConversionMap.put(rule, newMatch);
        return newMatch;
    }

    /**
     * cleans cache contents
     */
    @Override
    public void clear() {
        ruleMap.clear();
        ruleConversionMap.clear();
        absentTypes.clear();
        checkedTypes.clear();
        checkedRules.clear();
        unmatchableRules.clear();
    }


}
