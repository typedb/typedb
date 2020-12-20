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

package grakn.core.reasoner.resolution;

import grakn.common.collection.Pair;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.common.exception.GraknException;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Concludable;
import grakn.core.logic.resolvable.Resolvable;
import grakn.core.logic.resolvable.Retrievable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.logic.transformer.Mapping;
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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final HashMap<Concludable<?>, Actor<ConcludableResolver>> concludableActors;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private TraversalEngine traversalEngine;
    private final HashMap<Rule, Actor<RuleResolver>> rules;
    private EventLoopGroup elg;

    public ResolverRegistry(EventLoopGroup elg, Actor<ResolutionRecorder> resolutionRecorder, TraversalEngine traversalEngine) {
        this.elg = elg;
        this.resolutionRecorder = resolutionRecorder;
        this.traversalEngine = traversalEngine;
        rules = new HashMap<>();
        concludableActors = new HashMap<>();
    }

    public Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> registerResolvable(Resolvable resolvable) {
        if (resolvable.isRetrievable()) {
            return registerRetrievable(resolvable.asRetrievable());
        } else if (resolvable.isConcludable()) {
            return registerConcludable(resolvable.asConcludable());
        } else throw GraknException.of(ILLEGAL_STATE);
    }

    public Actor<RuleResolver> registerRule(Rule rule) {
        LOG.debug("Register retrieval for rule actor: '{}'", rule);
        return rules.computeIfAbsent(rule, (r) -> Actor.create(elg, self -> new RuleResolver(self, r, this, traversalEngine)));
    }

    public Actor<RootResolver> createRoot(final Conjunction pattern, final Consumer<ResolutionAnswer> onAnswer, Consumer<Integer> onExhausted) {
        LOG.debug("Creating Conjunction Actor for pattern: '{}'", pattern);
        return Actor.create(elg, self -> new RootResolver(self, pattern, onAnswer, onExhausted, resolutionRecorder, this, traversalEngine));
    }

    // for testing
    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.elg = eventLoopGroup;
    }

    private Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> registerRetrievable(Retrievable retrievable) {
        LOG.debug("Register retrieval for retrievable actor: '{}'", retrievable.conjunction());
        Actor<RetrievableResolver> retrievableActor = Actor.create(elg, self -> new RetrievableResolver(self, retrievable));
        return new Pair<>(retrievableActor, identity(retrievable));
    }

    private Pair<Actor<? extends ResolvableResolver<?>>, Map<Reference.Name, Reference.Name>> registerConcludable(Concludable<?> concludable) {
        LOG.debug("Register retrieval for concludable actor: '{}'", concludable.conjunction());
        for (Map.Entry<Concludable<?>, Actor<ConcludableResolver>> c: concludableActorsMap.entrySet()) {
            // TODO This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = c.getKey().alphaEquals(concludable);
            if (alphaEquality.isValid()) {
                return new Pair<>(c.getValue(), alphaEquality.asValid().namedVariableMapping());
            }
        }
        Actor<ConcludableResolver> concludableActor = Actor.create(elg, self -> new ConcludableResolver(self, concludable));
        concludableActors.put(concludable, concludableActor);
        return new Pair<>(concludableActor, identity(concludable));
    }

    private static Map<Reference.Name, Reference.Name> identity(Resolvable resolvable) {
        return new HashSet<>(resolvable.conjunction().variables()).stream()
                .filter(variable -> variable.reference().isName())
                .map(variable -> variable.reference().asName())
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
    }
}
