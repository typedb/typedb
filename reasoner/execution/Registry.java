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

package grakn.core.reasoner.execution;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.EventLoopGroup;
import grakn.core.reasoner.execution.actor.Concludable;
import grakn.core.reasoner.execution.actor.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class Registry {
    Logger LOG = LoggerFactory.getLogger(Registry.class);
    private final HashMap<Long, Actor<Concludable>> concludables;
    private final HashMap<List<Long>, Actor<Rule>> rules;
    private final Actor<AnswerRecorder> executionRecorder;

    public Registry(EventLoopGroup elg) {
        concludables = new HashMap<>();
        rules = new HashMap<>();
        executionRecorder = Actor.create(elg, AnswerRecorder::new);
    }

    public Actor<Concludable> registerConcludable(Long pattern, Function<Long, Actor<Concludable>> actorConstructor) {
        LOG.debug("Register retrieval for concludable actor: '{}'", pattern);
        return concludables.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<Rule> registerRule(List<Long> pattern, Function<List<Long>, Actor<Rule>> actorConstructor) {
        LOG.debug("Regstier retrieval for rule actor: '{}'", pattern);
        return rules.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<AnswerRecorder> executionRecorder() {
        return executionRecorder;
    }
}
