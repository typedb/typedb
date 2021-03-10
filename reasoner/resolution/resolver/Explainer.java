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

package grakn.core.reasoner.resolution.resolver;

import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.reasoner.resolution.answer.Explanation;
import grakn.core.reasoner.resolution.framework.Request;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.framework.Response;
import grakn.core.traversal.TraversalEngine;

import java.util.function.Consumer;

public class Explainer extends Resolver<Explainer> {

    private final Conjunction conjunction;
    private final ConceptMap bounds;
    private final Consumer<Explanation> requestAnswered;
    private final Consumer<Integer> requestFailed;
    private final Consumer<Throwable> exception;

    public Explainer(Driver<Explainer> driver, Conjunction conjunction, ConceptMap bounds, Consumer<Explanation> requestAnswered,
                     Consumer<Integer> requestFailed, Consumer<Throwable> exception, ResolverRegistry registry,
                     TraversalEngine traversalEngine, ConceptManager conceptMgr, boolean resolutionTracing) {
        super(driver, "Explainer(" + conjunction + " " + bounds, registry, traversalEngine, conceptMgr, resolutionTracing);
        this.conjunction = conjunction;
        this.bounds = bounds;
        this.requestAnswered = requestAnswered;
        this.requestFailed = requestFailed;
        this.exception = exception;
    }

    @Override
    public void receiveRequest(Request fromUpstream, int iteration) {

    }

    @Override
    protected void receiveAnswer(Response.Answer fromDownstream, int iteration) {

    }

    @Override
    protected void receiveFail(Response.Fail fromDownstream, int iteration) {

    }

    @Override
    protected void initialiseDownstreamResolvers() {

    }
}
