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

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.reasoner.resolution.framework.Answer;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
import grakn.core.reasoner.resolution.resolver.RuleResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

public class ResolverRegistry {

    private final static Logger LOG = LoggerFactory.getLogger(ResolverRegistry.class);

    private final HashMap<Long, Actor<ConcludableResolver>> concludables;
    private final HashMap<List<Long>, Actor<RuleResolver>> rules;
    private final Actor<ResolutionRecorder> resolutionRecorder;
    private final EventLoopGroup elg;

    public ResolverRegistry(EventLoopGroup elg) {
        this.elg = elg;
        concludables = new HashMap<>();
        rules = new HashMap<>();
        resolutionRecorder = Actor.create(elg, ResolutionRecorder::new);
    }

    public Actor<ConcludableResolver> registerConcludable(Long pattern, List<List<Long>> rules, long traversalSize) {
        LOG.debug("Register retrieval for concludable actor: '{}'", pattern);
        return concludables.computeIfAbsent(pattern, (p) -> Actor.create(elg, self -> new ConcludableResolver(self, p, rules, traversalSize)));
    }

    public Actor<RuleResolver> registerRule(List<Long> pattern, long traversalSize) {
        LOG.debug("Register retrieval for rule actor: '{}'", pattern);
        return rules.computeIfAbsent(pattern, (p) -> Actor.create(elg, self -> new RuleResolver(self, p, traversalSize)));
    }

    public Actor<RootResolver> createRoot(final List<Long> pattern, final long traversalSize, final Consumer<Answer> onAnswer,
                                          Runnable onExhausted) {
        LOG.debug("Creating Conjunction Actor for pattern: '{}'", pattern);
        return Actor.create(elg, self -> new RootResolver(self, pattern, traversalSize, onAnswer, onExhausted));
    }

    public Actor<ResolutionRecorder> resolutionRecorder() {
        return resolutionRecorder;
    }
}
