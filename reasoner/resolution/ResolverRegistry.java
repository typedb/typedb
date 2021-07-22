/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Negated;
import com.vaticle.typedb.core.logic.resolvable.Resolvable;
import com.vaticle.typedb.core.logic.resolvable.Retrievable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Explain;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Top.Match;
import com.vaticle.typedb.core.reasoner.resolution.framework.Resolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundRetrievableResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConcludableResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConclusionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConditionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.ConjunctionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.DisjunctionResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.NegationResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.RetrievableResolver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.RootResolver;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.RESOLUTION_TERMINATED;
import static java.util.stream.Collectors.toMap;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final ConceptManager conceptMgr;
    private final LogicManager logicMgr;
    private final Map<Pair<Actor.Driver<? extends Resolver<?>>, Integer>, Set<Actor.Driver<BoundConcludableResolver>>> reiterationQueryRespondents;
    private final Map<Concludable, Actor.Driver<ConcludableResolver>> concludableResolvers;
    private final ConcurrentMap<Rule, Actor.Driver<ConditionResolver>> ruleConditions;
    private final ConcurrentMap<Rule, Actor.Driver<ConclusionResolver>> ruleConclusions; // by Rule not Rule.Conclusion because well defined equality exists
    private final Set<Actor.Driver<? extends Resolver<?>>> resolvers;
    private final TraversalEngine traversalEngine;
    private final boolean resolutionTracing;
    private final AtomicBoolean terminated;
    private ActorExecutorGroup executorService;

    public ResolverRegistry(ActorExecutorGroup executorService, TraversalEngine traversalEngine, ConceptManager conceptMgr,
                            LogicManager logicMgr, boolean resolutionTracing) {
        this.executorService = executorService;
        this.traversalEngine = traversalEngine;
        this.conceptMgr = conceptMgr;
        this.logicMgr = logicMgr;
        this.concludableResolvers = new HashMap<>();
        this.reiterationQueryRespondents = new HashMap<>();
        this.ruleConditions = new ConcurrentHashMap<>();
        this.ruleConclusions = new ConcurrentHashMap<>();
        this.resolvers = new ConcurrentSet<>();
        this.terminated = new AtomicBoolean(false);
        this.resolutionTracing = resolutionTracing;
    }

    public Set<Actor.Driver<BoundConcludableResolver>> reiterationQueryRespondents(Actor.Driver<? extends Resolver<?>> root, int iteration) {
        return reiterationQueryRespondents.computeIfAbsent(new Pair<>(root, iteration), p -> new HashSet<>());
    }

    public void terminateResolvers(Throwable cause) {
        if (terminated.compareAndSet(false, true)) {
            resolvers.forEach(actor -> actor.execute(r -> r.terminate(cause)));
        }
    }

    public Actor.Driver<RootResolver.Conjunction> root(Conjunction conjunction, Consumer<Match.Finished> onAnswer,
                                                       Consumer<Integer> onFail, Consumer<Throwable> onException) {
        LOG.debug("Creating Root.Conjunction for: '{}'", conjunction);
        Actor.Driver<RootResolver.Conjunction> resolver = Actor.driver(driver -> new RootResolver.Conjunction(
                driver, conjunction, onAnswer, onFail, onException, this,
                traversalEngine, conceptMgr, logicMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<RootResolver.Disjunction> root(Disjunction disjunction, Consumer<Match.Finished> onAnswer,
                                                       Consumer<Integer> onExhausted, Consumer<Throwable> onException) {
        LOG.debug("Creating Root.Disjunction for: '{}'", disjunction);
        Actor.Driver<RootResolver.Disjunction> resolver = Actor.driver(driver -> new RootResolver.Disjunction(
                driver, disjunction, onAnswer, onExhausted, onException,
                this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public ResolverView.FilteredNegation negated(Negated negated, Conjunction upstream) {
        LOG.debug("Creating Negation resolver for : {}", negated);
        Actor.Driver<NegationResolver> negatedResolver = Actor.driver(driver -> new NegationResolver(
                driver, negated, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(negatedResolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        Set<Variable.Retrievable> filter = filter(upstream, negated);
        return ResolverView.negation(negatedResolver, filter);
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
                driver -> new ConditionResolver(driver, ruleCondition, this, traversalEngine,
                                                conceptMgr, logicMgr, resolutionTracing), executorService
        ));
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public Actor.Driver<ConclusionResolver> registerConclusion(Rule.Conclusion conclusion) {
        LOG.debug("Register retrieval for rule conclusion actor: '{}'", conclusion);
        Actor.Driver<ConclusionResolver> resolver = ruleConclusions.computeIfAbsent(conclusion.rule(), r -> Actor.driver(
                driver -> new ConclusionResolver(driver, conclusion, this,
                                                 traversalEngine, conceptMgr, resolutionTracing), executorService
        ));
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;

    }

    public ResolverView registerResolvable(Resolvable<?> resolvable) {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw TypeDBException.of(ILLEGAL_STATE);
    }

    private ResolverView.FilteredRetrievable registerRetrievable(com.vaticle.typedb.core.logic.resolvable.Retrievable retrievable) {
        LOG.debug("Register RetrievableResolver: '{}'", retrievable.pattern());
        Actor.Driver<RetrievableResolver> resolver = Actor.driver(driver -> new RetrievableResolver(
                driver, retrievable, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.retrievable(resolver, retrievable.retrieves());
    }

    public Actor.Driver<BoundRetrievableResolver> registerBoundRetrievable(Retrievable retrievable, ConceptMap bounds) {
        LOG.debug("Register BoundRetrievableResolver, pattern: {} bounds: {}", retrievable.pattern(), bounds);
        Actor.Driver<BoundRetrievableResolver> resolver = Actor.driver(driver -> new BoundRetrievableResolver(
                driver, retrievable, bounds, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<BoundConcludableResolver> registerBoundConcludable(
            Concludable concludable, ConceptMap bounds, Map<Actor.Driver<ConclusionResolver>, Rule> resolverRules,
            LinkedHashMap<Actor.Driver<ConclusionResolver>, Set<Unifier>> applicableRules,
            Actor.Driver<? extends Resolver<?>> root, int iteration) {
        LOG.debug("Register BoundConcludableResolver, pattern: {} bounds: {}", concludable.pattern(), bounds);
        Actor.Driver<BoundConcludableResolver> resolver = Actor.driver(driver -> new BoundConcludableResolver(
                driver, concludable, bounds, resolverRules, applicableRules, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        reiterationQueryRespondents.computeIfAbsent(new Pair<>(root, iteration), r -> new HashSet<>()).add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }
    // note: must be thread safe. We could move to a ConcurrentHashMap if we create an alpha-equivalence wrapper

    private synchronized ResolverView.MappedConcludable registerConcludable(Concludable concludable) {
        LOG.debug("Register ConcludableResolver: '{}'", concludable.pattern());
        for (Map.Entry<Concludable, Actor.Driver<ConcludableResolver>> c : concludableResolvers.entrySet()) {
            // TODO: This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = concludable.alphaEquals(c.getKey());
            if (alphaEquality.isValid()) {
                return ResolverView.concludable(c.getValue(), alphaEquality.asValid().idMapping());
            }
        }
        Actor.Driver<ConcludableResolver> resolver = Actor.driver(driver -> new ConcludableResolver(
                driver, concludable, this, traversalEngine,
                conceptMgr, logicMgr, resolutionTracing
        ), executorService);
        concludableResolvers.put(concludable, resolver);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return ResolverView.concludable(resolver, identity(concludable));
    }

    public Actor.Driver<ConjunctionResolver.Nested> nested(Conjunction conjunction) {
        LOG.debug("Creating Conjunction resolver for : {}", conjunction);
        Actor.Driver<ConjunctionResolver.Nested> resolver = Actor.driver(driver -> new ConjunctionResolver.Nested(
                driver, conjunction, this, traversalEngine, conceptMgr, logicMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public Actor.Driver<DisjunctionResolver.Nested> nested(Disjunction disjunction) {
        LOG.debug("Creating Disjunction resolver for : {}", disjunction);
        Actor.Driver<DisjunctionResolver.Nested> resolver = Actor.driver(driver -> new DisjunctionResolver.Nested(
                driver, disjunction, this, traversalEngine, conceptMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    private Map<Variable.Retrievable, Variable.Retrievable> identity(Resolvable<Conjunction> conjunctionResolvable) {
        return conjunctionResolvable.retrieves().stream().collect(toMap(Function.identity(), Function.identity()));
    }

    public Actor.Driver<RootResolver.Explain> explainer(Conjunction conjunction, Consumer<Explain.Finished> requestAnswered,
                                                        Consumer<Integer> requestFailed, Consumer<Throwable> exception) {
        Actor.Driver<RootResolver.Explain> resolver = Actor.driver(driver -> new RootResolver.Explain(
                driver, conjunction, requestAnswered, requestFailed, exception,
                this, traversalEngine, conceptMgr, logicMgr, resolutionTracing
        ), executorService);
        resolvers.add(resolver);
        if (terminated.get()) throw TypeDBException.of(RESOLUTION_TERMINATED); // guard races without synchronized
        return resolver;
    }

    public void setExecutorService(ActorExecutorGroup executorService) {
        this.executorService = executorService;
    }

    public static abstract class ResolverView {

        public static MappedConcludable concludable(Actor.Driver<ConcludableResolver> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
            return new MappedConcludable(resolver, mapping);
        }

        public static FilteredNegation negation(Actor.Driver<NegationResolver> resolver, Set<Variable.Retrievable> filter) {
            return new FilteredNegation(resolver, filter);
        }

        public static FilteredRetrievable retrievable(Actor.Driver<RetrievableResolver> resolver, Set<Variable.Retrievable> filter) {
            return new FilteredRetrievable(resolver, filter);
        }

        public boolean isMappedConcludable() { return false; }

        public boolean isFilteredNegation() { return false; }

        public boolean isFilteredRetrievable() { return false; }

        public MappedConcludable asMappedConcludable() {
            throw TypeDBException.of(ILLEGAL_CAST, getClass(), MappedConcludable.class);
        }

        public FilteredNegation asFilteredNegation() {
            throw TypeDBException.of(ILLEGAL_CAST, getClass(), FilteredNegation.class);
        }

        public FilteredRetrievable asFilteredRetrievable() {
            throw TypeDBException.of(ILLEGAL_CAST, getClass(), FilteredRetrievable.class);
        }

        public abstract Actor.Driver<? extends Resolver<?>> resolver();

        public static class MappedConcludable extends ResolverView {
            private final Actor.Driver<ConcludableResolver> resolver;
            private final Map<Variable.Retrievable, Variable.Retrievable> mapping;

            public MappedConcludable(Actor.Driver<ConcludableResolver> resolver, Map<Variable.Retrievable, Variable.Retrievable> mapping) {
                this.resolver = resolver;
                this.mapping = mapping;
            }

            public Map<Variable.Retrievable, Variable.Retrievable> mapping() {
                return mapping;
            }

            @Override
            public boolean isMappedConcludable() {
                return true;
            }

            @Override
            public MappedConcludable asMappedConcludable() {
                return this;
            }

            @Override
            public Actor.Driver<ConcludableResolver> resolver() {
                return resolver;
            }
        }

        public static class FilteredNegation extends ResolverView {
            private final Actor.Driver<NegationResolver> resolver;
            private final Set<Variable.Retrievable> filter;

            public FilteredNegation(Actor.Driver<NegationResolver> resolver, Set<Variable.Retrievable> filter) {
                this.resolver = resolver;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public boolean isFilteredNegation() {
                return true;
            }

            @Override
            public FilteredNegation asFilteredNegation() {
                return this;
            }

            @Override
            public Actor.Driver<? extends Resolver<?>> resolver() {
                return resolver;
            }
        }

        public static class FilteredRetrievable extends ResolverView {
            private final Actor.Driver<RetrievableResolver> resolver;
            private Set<Variable.Retrievable> filter;

            public FilteredRetrievable(Actor.Driver<RetrievableResolver> resolver, Set<Variable.Retrievable> filter) {
                this.resolver = resolver;
                this.filter = filter;
            }

            public Set<Variable.Retrievable> filter() {
                return filter;
            }

            @Override
            public boolean isFilteredRetrievable() { return true; }

            @Override
            public FilteredRetrievable asFilteredRetrievable() {
                return this;
            }

            @Override
            public Actor.Driver<RetrievableResolver> resolver() {
                return resolver;
            }
        }
    }
}
