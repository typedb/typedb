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
import grakn.core.concept.logic.Rule;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.pattern.variable.Variable;
import grakn.core.reasoner.concludable.ConjunctionConcludable;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.reasoner.resolution.resolver.RuleResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
public class ResolverRegistry {

    Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);
    private final HashMap<ConjunctionConcludable<?, ?>, Actor<ConcludableResolver>> concludableActorsMap;
    private final HashMap<Rule, Actor<RuleResolver>> rules;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final EventLoopGroup elg;

    public ResolverRegistry(EventLoopGroup elg) {
        this.elg = elg;
        concludableActorsMap = new HashMap<>();
        rules = new HashMap<>();
        resolutionRecorder = Actor.create(elg, ResolutionRecorder::new);
    }

    public Pair<Actor<ConcludableResolver>, Unifier> registerConcludable(ConjunctionConcludable<?, ?> concludable, List<Rule> rules, long traversalSize) {
        LOG.debug("Register retrieval for concludable actor: '{}'", concludable.conjunction());

        final Optional<Pair<ConjunctionConcludable<?, ?>, AlphaEquivalence>> alphaEquivalencePair = concludableActorsMap.keySet().stream()
                .map(k -> new Pair<ConjunctionConcludable<?, ?>, AlphaEquivalence>(k, k.alphaEquals(concludable)))
                .filter(p -> p.second().isValid()).findAny();

        final Actor<ConcludableResolver> concludableActor;
        final Unifier unifier;
        if (alphaEquivalencePair.isPresent()) {
            // Then we can use the same ConcludableActor, but with a different variable mapping
            concludableActor = concludableActorsMap.get(alphaEquivalencePair.get().first());
            unifier = Unifier.fromVariableMapping(alphaEquivalencePair.get().second().asValid().map());
        } else {
            // Create a new ConcludableActor
            concludableActor = Actor.create(elg, self -> new ConcludableResolver(self, concludable.conjunction(), rules, traversalSize));
            concludableActorsMap.put(concludable, concludableActor);
            final Set<Variable> vars = new HashSet<>(concludable.constraint().variables());
            unifier = Unifier.identity(vars);
        }
        return new Pair<>(concludableActor, unifier);
    }

    public Actor<RuleResolver> registerRule(Rule rule, long traversalSize) {
        LOG.debug("Register retrieval for rule actor: '{}'", rule);
        return rules.computeIfAbsent(rule, (p) -> Actor.create(elg, self -> new RuleResolver(self, p.when(), traversalSize)));
    }

    public Actor<RootResolver> createRoot(final Conjunction pattern, final Consumer<ResolutionAnswer> onAnswer, Runnable onExhausted) {
        LOG.debug("Creating Conjunction Actor for pattern: '{}'", pattern);
        return Actor.create(elg, self -> new RootResolver(self, pattern, null, onAnswer, onExhausted));
    }

    public Actor<ResolutionRecorder> resolutionRecorder() {
        return resolutionRecorder;
    }
}
