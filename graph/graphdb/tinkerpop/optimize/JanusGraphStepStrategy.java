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
 */

package grakn.core.graph.graphdb.tinkerpop.optimize;

import grakn.core.graph.graphdb.tinkerpop.ElementUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

@SuppressWarnings("ComparableType")
public class JanusGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphStepStrategy INSTANCE = new JanusGraphStepStrategy();

    private JanusGraphStepStrategy() {
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal)) {
            return;
        }

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
                //Try to optimize for index calls
                JanusGraphStep<?, ?> janusGraphStep = new JanusGraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(originalGraphStep, janusGraphStep, traversal);
                HasStepFolder.foldInIds(janusGraphStep, traversal);
                HasStepFolder.foldInHasContainer(janusGraphStep, traversal, traversal);
                HasStepFolder.foldInOrder(janusGraphStep, janusGraphStep.getNextStep(), traversal, traversal, janusGraphStep.returnsVertex(), null);
                HasStepFolder.foldInRange(janusGraphStep, JanusGraphTraversalUtil.getNextNonIdentityStep(janusGraphStep), traversal, null);
            } else {
                //Make sure that any provided "start" elements are instantiated in the current transaction
                Object[] ids = originalGraphStep.getIds();
                ElementUtils.verifyArgsMustBeEitherIdOrElement(ids);
                if (ids[0] instanceof Element) {
                    //GraphStep constructor ensures that the entire array is elements
                    final Object[] elementIds = new Object[ids.length];
                    for (int i = 0; i < ids.length; i++) {
                        elementIds[i] = ((Element) ids[i]).id();
                    }
                    originalGraphStep.setIteratorSupplier(() -> originalGraphStep.returnsVertex() ?
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
                            ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds));
                }
            }

        });
    }

    public static JanusGraphStepStrategy instance() {
        return INSTANCE;
    }
}
