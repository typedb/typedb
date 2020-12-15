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
import grakn.core.logic.Rule;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.equivalence.AlphaEquivalence;
import grakn.core.reasoner.resolution.answer.Mapping;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.reasoner.resolution.resolver.RuleResolver;
import graql.lang.pattern.variable.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final HashMap<ConjunctionConcludable<?, ?>, Actor<ConcludableResolver>> concludableActorsMap;
    private final HashMap<Rule, Actor<RuleResolver>> rules;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private EventLoopGroup elg;

    public ResolverRegistry(EventLoopGroup elg) {
        this.elg = elg;
        concludableActorsMap = new HashMap<>();
        rules = new HashMap<>();
        resolutionRecorder = Actor.create(elg, ResolutionRecorder::new);
    }

    public Pair<Actor<ConcludableResolver>, Map<Reference.Name, Reference.Name>> registerConcludable(ConjunctionConcludable<?, ?> concludable) {
        LOG.debug("Register retrieval for concludable actor: '{}'", concludable.conjunction());
        for (Map.Entry<ConjunctionConcludable<?, ?>, Actor<ConcludableResolver>> c: concludableActorsMap.entrySet()) {
            // TODO This needs to be optimised from a linear search to use an alpha hash
            AlphaEquivalence alphaEquality = c.getKey().alphaEquals(concludable);
            if (alphaEquality.isValid()) {
                return new Pair<>(c.getValue(), alphaEquality.asValid().namedVariableMapping());
            }
        }
        Actor<ConcludableResolver> concludableActor = Actor.create(elg, self -> new ConcludableResolver(self, concludable));
        concludableActorsMap.put(concludable, concludableActor);
        return new Pair<>(concludableActor, Mapping.identity(concludable));
    }

    public Actor<RuleResolver> registerRule(Rule rule) {
        LOG.debug("Register retrieval for rule actor: '{}'", rule);
        return rules.computeIfAbsent(rule, (r) -> Actor.create(elg, self -> new RuleResolver(self, r)));
    }

    public Actor<RootResolver> createRoot(final Conjunction pattern, final Consumer<ResolutionAnswer> onAnswer, Runnable onExhausted) {
        LOG.debug("Creating Conjunction Actor for pattern: '{}'", pattern);
        return Actor.create(elg, self -> new RootResolver(self, pattern, onAnswer, onExhausted));
    }

    public Actor<ResolutionRecorder> resolutionRecorder() {
        return resolutionRecorder;
    }

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.elg = eventLoopGroup;
    }
}
