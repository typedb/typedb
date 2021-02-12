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

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concurrent.actor.Actor;
import grakn.core.concurrent.actor.EventLoopGroup;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Negated;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.ConjunctionResolver;
import grakn.core.reasoner.resolution.resolver.NegationResolver;
import grakn.core.reasoner.resolution.resolver.RetrievableResolver;
import grakn.core.reasoner.resolution.resolver.Root;
import grakn.core.reasoner.resolution.resolver.RuleResolver;
import grakn.core.traversal.TraversalEngine;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final ConceptManager conceptMgr;
    private final HashMap<Concludable, Actor<ConcludableResolver>> concludableActors;
    private final LogicManager logicMgr;
    private boolean explanations;
    private final HashMap<Rule, Actor<RuleResolver>> rules;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private EventLoopGroup elg;
    private final Planner planner;

    public ResolverRegistry(EventLoopGroup elg, Actor<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine,
                            ConceptManager conceptMgr, LogicManager logicMgr) {
        this.elg = elg;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.explanations = false; // TODO: enable/disable explanations from transaction context
        concludableActors = new HashMap<>();
        rules = new HashMap<>();
        planner = new Planner(conceptMgr, logicMgr);
    }

    public MappedResolver registerResolvable(Resolvable resolvable) {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    public Actor<RuleResolver> registerRule(Rule rule) {
        LOG.debug("Register retrieval for rule actor: '{}'", rule);
        return rules.computeIfAbsent(rule, (r) -> Actor.create(elg, self -> new RuleResolver(
                self, r, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, planner,
                explanations)));
    }

    public Actor<Root.Conjunction> rootConjunction(Conjunction conjunction, Set<Reference.Name> filter, @Nullable Long offset,
                                                   @Nullable Long limit, Consumer<ResolutionAnswer> onAnswer,
                                                   Consumer<Integer> onExhausted) {
        LOG.debug("Creating Root.Conjunction for: '{}'", conjunction);
        return Actor.create(
                elg, self -> new Root.Conjunction(
                        self, conjunction, filter, offset, limit, onAnswer, onExhausted, resolutionRecorder, this, traversalEngine,
                        conceptMgr, logicMgr, planner, explanations));
    }

    public Actor<Root.Disjunction> rootDisjunction(Disjunction disjunction, Set<Reference.Name> filter, @Nullable Long offset,
                                                   @Nullable Long limit, Consumer<ResolutionAnswer> onAnswer,
                                                   Consumer<Integer> onExhausted) {
        LOG.debug("Creating Root.Disjunction for: '{}'", disjunction);
        return Actor.create(
                elg, self -> new Root.Disjunction(self, disjunction, filter, offset, limit, onAnswer, onExhausted, resolutionRecorder,
                                                  this, traversalEngine, explanations)
        );
    }

    private MappedResolver registerRetrievable(Retrievable retrievable) {
        LOG.debug("Register RetrievableResolver: '{}'", retrievable.pattern());
        Actor<RetrievableResolver> retrievableActor = Actor.create(elg, self -> new RetrievableResolver(
                self, retrievable, this, traversalEngine, conceptMgr, explanations));
        return MappedResolver.of(retrievableActor, identity(retrievable));
    }

    // note: must be thread safe. We could move to a ConcurrentHashMap if we create an alpha-equivalence wrapper
    private synchronized MappedResolver registerConcludable(Concludable concludable) {
        LOG.debug("Register ConcludableResolver: '{}'", concludable.pattern());
        for (Map.Entry<Concludable, Actor<ConcludableResolver>> c : concludableActors.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = concludable.alphaEquals(c.getKey());
            if (alphaEquality.isValid()) {
                return MappedResolver.of(c.getValue(), alphaEquality.asValid().namedVariableMapping());
            }
        }
        Actor<ConcludableResolver> concludableActor = Actor.create(elg, self ->
                new ConcludableResolver(self, concludable, resolutionRecorder, this, traversalEngine, conceptMgr,
                                        logicMgr, explanations));
        concludableActors.put(concludable, concludableActor);
        return MappedResolver.of(concludableActor, identity(concludable));
    }

    public Actor<ConjunctionResolver.Nested> conjunction(Conjunction conjunction) {
        LOG.debug("Creating Conjunction resolver for : {}", conjunction);
        return Actor.create(
                elg, self -> new ConjunctionResolver.Nested(
                        self, conjunction, resolutionRecorder, this, traversalEngine, conceptMgr, logicMgr, planner,
                        explanations)
        );
    }

    public MappedResolver negated(Negated negated, Conjunction upstream) {
        LOG.debug("Creating Negation resolver for : {}", negated);
        Actor<NegationResolver> negatedResolver = Actor.create(
                elg, self -> new NegationResolver(self, negated, this, traversalEngine, resolutionRecorder, explanations)
        );
        Map<Reference.Name, Reference.Name> filteredMapping = identityFiltered(upstream, negated);
        return MappedResolver.of(negatedResolver, filteredMapping);
    }

    private Map<Reference.Name, Reference.Name> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.namedVariables().stream()
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    private Map<Reference.Name, Reference.Name> identityFiltered(Conjunction upstream, Negated negated) {
        return upstream.variables().stream()
                .filter(var -> var.reference().isName() && negated.namedVariables().contains(var))
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    // for testing

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.elg = eventLoopGroup;
    }

    public static class MappedResolver {
        private final Actor<? extends Resolver<?>> resolver;
        private final Map<Reference.Name, Reference.Name> mapping;

        private MappedResolver(Actor<? extends Resolver<?>> resolver, Map<Reference.Name, Reference.Name> mapping) {
            this.resolver = resolver;
            this.mapping = mapping;
        }

        public static MappedResolver of(Actor<? extends Resolver<?>> resolver, Map<Reference.Name, Reference.Name> mapping) {
            return new MappedResolver(resolver, mapping);
        }

        public Map<Reference.Name, Reference.Name> mapping() {
            return mapping;
        }

        public Actor<? extends Resolver<?>> resolver() {
            return resolver;
        }
    }
}
