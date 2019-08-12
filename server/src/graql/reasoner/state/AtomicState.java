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

package grakn.core.graql.reasoner.state;

import com.google.common.collect.HashMultimap;
import grakn.core.concept.ConceptId;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.cache.CacheEntry;
import grakn.core.graql.reasoner.cache.IndexedAnswerSet;
import grakn.core.graql.reasoner.explanation.RuleExplanation;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueries;
import grakn.core.graql.reasoner.rule.InferenceRule;
import grakn.core.graql.reasoner.unifier.MultiUnifier;
import grakn.core.graql.reasoner.unifier.Unifier;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.server.kb.concept.ConceptUtils;
import graql.lang.statement.Variable;
import java.util.Iterator;
import java.util.Set;

/**
 * Query state corresponding to an atomic query (ReasonerAtomicQuery) in the resolution tree.
 */
public class AtomicState extends AnswerPropagatorState<ReasonerAtomicQuery> {

    //TODO: remove it once we introduce multi answer states
    private MultiUnifier ruleUnifier = null;

    private MultiUnifier cacheUnifier = null;
    private CacheEntry<ReasonerAtomicQuery, IndexedAnswerSet> cacheEntry = null;
    final private HashMultimap<ConceptId, ConceptMap> materialised = HashMultimap.create();

    public AtomicState(ReasonerAtomicQuery query,
                ConceptMap sub,
                Unifier u,
                AnswerPropagatorState parent,
                Set<ReasonerAtomicQuery> subGoals) {
        super(ReasonerQueries.atomic(query, sub), sub, u, parent, subGoals);
    }

    @Override
    public String toString(){ return super.toString() + "\n" + getQuery() + "\n"; }

    @Override
    Iterator<ResolutionState> generateChildStateIterator() {
        return getQuery().innerStateIterator(this, getVisitedSubGoals());
    }

    @Override
    ResolutionState propagateAnswer(AnswerState state) {
        ConceptMap answer = consumeAnswer(state);
        ReasonerAtomicQuery query = getQuery();
        if (answer.isEmpty()) return null;

        if (state.getRule() != null && query.getAtom().requiresRoleExpansion()) {
            //NB: we set the parent state as this AtomicState, otherwise we won't acknowledge expanded answers (won't cache)
            return new RoleExpansionState(answer, getUnifier(), query.getAtom().getRoleExpansionVariables(), this);
        }
        return new AnswerState(answer, getUnifier(), getParentState());
    }

    @Override
    ConceptMap consumeAnswer(AnswerState state) {
        ConceptMap answer;
        ReasonerAtomicQuery query = getQuery();
        ConceptMap baseAnswer = state.getSubstitution();
        InferenceRule rule = state.getRule();
        Unifier unifier = state.getUnifier();
        if (rule == null) {
            answer = ConceptUtils.mergeAnswers(baseAnswer, query.getSubstitution())
                    .project(query.getVarNames());
        } else {
            answer = rule.requiresMaterialisation(query.getAtom()) ?
                    materialisedAnswer(baseAnswer, rule, unifier) :
                    ruleAnswer(baseAnswer, rule, unifier);
        }
        return recordAnswer(query, answer);
    }

    /**
     * @return cache unifier if any
     */
    private MultiUnifier getCacheUnifier() {
        if (cacheUnifier == null) this.cacheUnifier = getQuery().tx().queryCache().getCacheUnifier(getQuery());
        return cacheUnifier;
    }

    private MultiUnifier getRuleUnifier(InferenceRule rule) {
        if (ruleUnifier == null) this.ruleUnifier = rule.getHead().getMultiUnifier(this.getQuery(), UnifierType.RULE);
        return ruleUnifier;
    }

    private ConceptMap recordAnswer(ReasonerAtomicQuery query, ConceptMap answer) {
        if (answer.isEmpty()) return answer;
        if (cacheEntry == null) {
            cacheEntry = getQuery().tx().queryCache().record(query, answer, cacheEntry, null);
            return answer;
        }
        getQuery().tx().queryCache().record(query, answer, cacheEntry, getCacheUnifier());
        return answer;
    }

    private ConceptMap ruleAnswer(ConceptMap baseAnswer, InferenceRule rule, Unifier unifier) {
        ReasonerAtomicQuery query = getQuery();
        ConceptMap answer = unifier.apply(ConceptUtils.mergeAnswers(
                baseAnswer, rule.getHead().getRoleSubstitution())
        );
        if (answer.isEmpty()) return answer;

        return ConceptUtils.mergeAnswers(answer, query.getSubstitution())
                .project(query.getVarNames())
                .explain(new RuleExplanation(query.getPattern(), rule.getRule().id()));
    }

    private ConceptMap materialisedAnswer(ConceptMap baseAnswer, InferenceRule rule, Unifier unifier) {
        ConceptMap answer = baseAnswer;
        ReasonerAtomicQuery query = getQuery();
        ReasonerAtomicQuery ruleHead = ReasonerQueries.atomic(rule.getHead(), answer);
        ConceptMap sub = ruleHead.getSubstitution();
        if(materialised.get(rule.getRule().id()).contains(sub)
            && getRuleUnifier(rule).isUnique()){
            return new ConceptMap();
        }
        materialised.put(rule.getRule().id(), sub);

        Set<Variable> headVars = query.getVarNames().size() < ruleHead.getVarNames().size() ?
                unifier.keySet() :
                ruleHead.getVarNames();

        //materialise exhibits put behaviour - duplicates won't be created
        ConceptMap materialisedSub = ruleHead.materialise(answer).findFirst().orElse(null);
        if (materialisedSub != null) {
            RuleExplanation ruleExplanation = new RuleExplanation(query.getPattern(), rule.getRule().id());
            ConceptMap ruleAnswer = materialisedSub.explain(ruleExplanation);
            getQuery().tx().queryCache().record(ruleHead, ruleAnswer);
            Atom ruleAtom = ruleHead.getAtom();
            //if it's an implicit relation also record it as an attribute
            if (ruleAtom.isRelation() && ruleAtom.getSchemaConcept() != null && ruleAtom.getSchemaConcept().isImplicit()) {
                ReasonerAtomicQuery attributeHead = ReasonerQueries.atomic(ruleHead.getAtom().toAttributeAtom());
                getQuery().tx().queryCache().record(attributeHead, ruleAnswer.project(attributeHead.getVarNames()));
            }
            answer = unifier.apply(materialisedSub.project(headVars));
        }
        if (answer.isEmpty()) return answer;

        return ConceptUtils
                .mergeAnswers(answer, query.getSubstitution())
                .project(query.getVarNames())
                .explain(new RuleExplanation(query.getPattern(), rule.getRule().id()));
    }
}
