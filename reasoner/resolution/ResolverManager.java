/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution;

import grakn.core.common.concurrent.actor.Actor;
import grakn.core.common.concurrent.actor.EventLoopGroup;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.ResolvableResolver;
import grakn.core.reasoner.resolution.resolver.RetrievableResolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.reasoner.resolution.resolver.RuleResolver;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ResolverManager {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverManager.class);

    private final ConceptManager conceptMgr;
    private final HashMap<Concludable, Actor<ConcludableResolver>> concludableActors;
    private final LogicManager logicMgr;
    private boolean explanations;
    private final HashMap<Rule, Actor<RuleResolver>> rules;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private EventLoopGroup elg;

    public ResolverManager(EventLoopGroup elg, Actor<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine,
                           ConceptManager conceptMgr, LogicManager logicMgr) {
        this.elg = elg;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.explanations = false; // TODO enable/disable explanations from transaction context
        concludableActors = new HashMap<>();
        rules = new HashMap<>();
    }

    public List<MappedActor> planAndRegister(Set<Resolvable> resolvables) {
        return iterate(new Planner(conceptMgr, logicMgr).plan(resolvables)).map(this::registerResolvable).toList();
    }

    public Actor<RuleResolver> registerRule(Rule rule) {
        LOG.debug("Register retrieval for rule actor: '{}'", rule);
        return rules.computeIfAbsent(rule, (r) -> Actor.create(elg, self -> new RuleResolver(self, r, this, traversalEngine, conceptMgr, logicMgr, explanations)));
    }

    public Actor<RootResolver> createRoot(final Conjunction pattern, final Consumer<ResolutionAnswer> onAnswer, Consumer<Integer> onExhausted) {
        LOG.debug("Creating Conjunction Actor for pattern: '{}'", pattern);
        return Actor.create(elg, self -> new RootResolver(self, pattern, onAnswer, onExhausted, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, explanations));
    }

    // for testing
    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.elg = eventLoopGroup;
    }

    private MappedActor registerResolvable(Resolvable resolvable) {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private MappedActor registerRetrievable(Retrievable retrievable) {
        LOG.debug("Register retrieval for retrievable actor: '{}'", retrievable.conjunction());
        Actor<RetrievableResolver> retrievableActor = Actor.create(elg, self -> new RetrievableResolver(self, retrievable, this, traversalEngine, explanations));
        return new MappedActor(retrievableActor, identity(retrievable));
    }

    private MappedActor registerConcludable(Concludable concludable) {
        LOG.debug("Register retrieval for concludable actor: '{}'", concludable.conjunction());
        for (Map.Entry<Concludable, Actor<ConcludableResolver>> c : concludableActors.entrySet()) {
            // TODO This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = c.getKey().alphaEquals(concludable);
            if (alphaEquality.isValid()) {
                return new MappedActor(c.getValue(), alphaEquality.asValid().namedVariableMapping());
            }
        }
        Actor<ConcludableResolver> concludableActor = Actor.create(elg, self ->
                new ConcludableResolver(self, concludable, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, explanations));
        concludableActors.put(concludable, concludableActor);
        return new MappedActor(concludableActor, identity(concludable));
    }

    private static Map<Reference.Name, Reference.Name> identity(Resolvable resolvable) {
        return new HashSet<>(resolvable.conjunction().variables()).stream()
                .filter(variable -> variable.reference().isName())
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    public static class MappedActor {
        private final Actor<? extends ResolvableResolver<?>> actor;
        private final Map<Reference.Name, Reference.Name> mapping;

        public MappedActor(Actor<? extends ResolvableResolver<?>> actor, Map<Reference.Name, Reference.Name> mapping) {
            this.actor = actor;
            this.mapping = mapping;
        }

        public Map<Reference.Name, Reference.Name> mapping() {
            return mapping;
        }

        public Actor<? extends ResolvableResolver<?>> actor() {
            return actor;
        }
    }

}
