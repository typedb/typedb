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
import grakn.core.concurrent.actor.ActorExecutorGroup;
import grakn.common.collection.ConcurrentSet;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Negated;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.ConclusionResolver;
import grakn.core.reasoner.resolution.resolver.ConditionResolver;
import grakn.core.reasoner.resolution.resolver.ConjunctionResolver;
import grakn.core.reasoner.resolution.resolver.DisjunctionResolver;
import grakn.core.reasoner.resolution.resolver.NegationResolver;
import grakn.core.reasoner.resolution.resolver.RetrievableResolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.traversal.TraversalEngine;
import grakn.core.traversal.common.Identifier.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Reasoner.RESOLUTION_TERMINATED;
import static java.util.stream.Collectors.toMap;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Concludable, Actor.Driver<ConcludableResolver>> concludableResolvers;
    private final ConcurrentMap<Rule, Actor.Driver<ConditionResolver>> ruleConditions;
    private final ConcurrentMap<Rule, Actor.Driver<ConclusionResolver>> ruleConclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final Set<Actor.Driver<? extends Resolver<?>>> resolvers;
    private final Actor.Driver<ResolutionRecorder> resolutionRecorder;
    private final TraversalEngine traversalEngine;
    private final Planner planner;
    private final boolean resolutionTracing;
    private ActorExecutorGroup executorService;
    private AtomicBoolean terminated;
    private boolean explanations;

    public ResolverRegistry(ActorExecutorGroup executorService, Actor.Driver<ResolutionRecorder> resolutionRecorder,
                            TraversalEngine traversalEngine, ConceptManager conceptMgr, LogicManager logicMgr,
                            boolean resolutionTracing) {
        this.executorService = executorService;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableResolvers = new HashMap<>();
        this.ruleConditions = new ConcurrentHashMap<>();
        this.ruleConclusions = new ConcurrentHashMap<>();
        this.resolvers = new ConcurrentSet<>();
        this.planner = new Planner(conceptMgr, logicMgr);
        this.terminated = new AtomicBoolean(false);
        this.resolutionTracing = resolutionTracing;
    }

    public void terminateResolvers(Throwable cause) {
        if (terminated.compareAndSet(false, true)) {
            resolvers.forEach(actor -> {
                actor.execute(r -> r.terminate(cause));
            });
        }
    }

    public Actor.Driver<RootResolver.Conjunction> root(Conjunction conjunction, Consumer<Top> onAnswer,
                                                       Consumer<Integer> onFail, Consumer<Throwable> onException) {
        LOG.debug("Creating Root.Conjunction for: '{}'", conjunction);
        Actor.Driver<RootResolver.Conjunction> resolver = Actor.driver(driver -> new RootResolver.Conjunction(
                driver, conjunction, onAnswer, onFail, onException, resolutionRecorder, this,
                traversalEngine, conceptMgr, logicMgr, planner, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<RootResolver.Disjunction> root(Disjunction disjunction, Consumer<Top> onAnswer,
                                                       Consumer<Integer> onExhausted, Consumer<Throwable> onException) {
        LOG.debug("Creating Root.Disjunction for: '{}'", disjunction);
        Actor.Driver<RootResolver.Disjunction> resolver = Actor.driver(driver -> new RootResolver.Disjunction(
                driver, disjunction, onAnswer, onExhausted, onException,
                resolutionRecorder, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public ResolverView.Filtered negated(Negated negated, Conjunction upstream) {
        LOG.debug("Creating Negation resolver for : {}", negated);
        Actor.Driver<NegationResolver> negatedResolver = Actor.driver(driver -> new NegationResolver(
                driver, negated, this, traversalEngine, conceptMgr, resolutionRecorder, resolutionTracing
        ), executorService);
        resolvers.add(negatedResolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        Set<Variable.Retrievable> filter = filter(upstream, negated);
        return ResolverView.filtered(negatedResolver, filter);
    }

    private Set<Variable.Retrievable> filter(Conjunction scope, Negated inner) {
        return scope.variables().stream()
                .filter(var -> var.id().isRetrievable() && inner.retrieves().contains(var.id().asRetrievable()))
                .map(var -> var.id().asRetrievable())
                .collect(Collectors.toSet());
    }

    public Actor.Driver<ConditionResolver> registerCondition(Rule.Condition ruleCondition) {
        LOG.debug("Register retrieval for rule condition actor: '{}'", ruleCondition);
        Actor.Driver<ConditionResolver> resolver = ruleConditions.computeIfAbsent(ruleCondition.rule(), (r) -> Actor.driver(
                driver -> new ConditionResolver(driver, ruleCondition, resolutionRecorder, this, traversalEngine,
                                                conceptMgr, logicMgr, planner, resolutionTracing), executorService
        ));
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public Actor.Driver<ConclusionResolver> registerConclusion(Rule.Conclusion conclusion) {
        LOG.debug("Register retrieval for rule conclusion actor: '{}'", conclusion);
        Actor.Driver<ConclusionResolver> resolver = ruleConclusions.computeIfAbsent(conclusion.rule(), r -> Actor.driver(
                driver -> new ConclusionResolver(driver, conclusion, this, resolutionRecorder,
                                                 traversalEngine, conceptMgr, resolutionTracing), executorService
        ));
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public ResolverView registerResolvable(Resolvable<?> resolvable) {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    private ResolverView.Filtered registerRetrievable(Retrievable retrievable) {
        LOG.debug("Register RetrievableResolver: '{}'", retrievable.pattern());
        Actor.Driver<RetrievableResolver> resolver = Actor.driver(driver -> new RetrievableResolver(
                driver, retrievable, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.filtered(resolver, retrievable.retrieves());
    }

    // note: must be thread safe. We could move to a ConcurrentHashMap if we create an alpha-equivalence wrapper
    private synchronized ResolverView.Mapped registerConcludable(Concludable concludable) {
        LOG.debug("Register ConcludableResolver: '{}'", concludable.pattern());
        for (Map.Entry<Concludable, Actor.Driver<ConcludableResolver>> c : concludableResolvers.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = concludable.alphaEquals(c.getKey());
            if (alphaEquality.isValid()) {
                return ResolverView.mapped(c.getValue(), alphaEquality.asValid().idMapping());
            }
        }
        Actor.Driver<ConcludableResolver> resolver = Actor.driver(driver -> new ConcludableResolver(
                driver, concludable, resolutionRecorder, this, traversalEngine,
                conceptMgr, logicMgr, resolutionTracing
        ), executorService);
        concludableResolvers.put(concludable, resolver);
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.mapped(resolver, identity(concludable));
    }

    public Actor.Driver<ConjunctionResolver.Nested> nested(Conjunction conjunction) {
        LOG.debug("Creating Conjunction resolver for : {}", conjunction);
        Actor.Driver<ConjunctionResolver.Nested> resolver = Actor.driver(driver -> new ConjunctionResolver.Nested(
                driver, conjunction, resolutionRecorder, this, traversalEngine,
                conceptMgr, logicMgr, planner, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw GraknException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<DisjunctionResolver.Nested> nested(Disjunction disjunction) {
        LOG.debug("Creating Disjunction resolver for : {}", disjunction);
        return Actor.driver(driver -> new DisjunctionResolver.Nested(
                driver, disjunction, resolutionRecorder, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
    }

    private Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream().collect(toMap(Function.identity(), Function.identity()));
    }

    public void setExecutorService(ActorExecutorGroup executorService) {
        this.executorService = executorService;
    }

    public static abstract class ResolverView {

        public static Mapped mapped(Actor.Driver<ConcludableResolver> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new Mapped(resolver, mapping);
        }

        public static Filtered filtered(Actor.Driver<? extends Resolver<?>> resolver, Set<Variable.Retrievable> filter) {
            return new Filtered(resolver, filter);
        }

        public abstract boolean isMapped();

        public abstract boolean isFiltered();

        public Mapped asMapped() {
            throw GraknException.of(ILLEGAL_CAST, getClass(), Mapped.class);
        }

        public Filtered asFiltered() {
            throw GraknException.of(ILLEGAL_CAST, getClass(), Mapped.class);
        }

        public abstract Actor.Driver<? extends Resolver<?>> resolver();

        public static class Mapped extends ResolverView {
            private final Actor.Driver<ConcludableResolver> resolver;
            private final Map<Variable.Retrievable, Variable.Retrievable> mapping;

            public Mapped(Actor.Driver<ConcludableResolver> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
                this.resolver = resolver;
                this.mapping = mapping;
            }

            public Map<Variable.Retrievable, Variable.Retrievable> mapping() {
                return mapping;
            }

            @Override
            public boolean isMapped() {
                return true;
            }

            @Override
            public boolean isFiltered() {
                return false;
            }

            @Override
            public Mapped asMapped() {
                return this;
            }

            @Override
            public Actor.Driver<ConcludableResolver> resolver() {
                return resolver;
            }
        }

        public static class Filtered extends ResolverView {
            private final Actor.Driver<? extends Resolver<?>> resolver;
            private final Set<Variable.Retrievable> filter;

            public Filtered(Actor.Driver<? extends Resolver<?>> resolver, Set<Variable.Retrievable> filter) {
                this.resolver = resolver;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public boolean isMapped() {
                return false;
            }

            @Override
            public boolean isFiltered() {
                return true;
            }

            @Override
            public Filtered asFiltered() {
                return this;
            }

            @Override
            public Actor.Driver<? extends Resolver<?>> resolver() {
                return resolver;
            }
        }
    }
}
