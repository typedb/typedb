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

package grakn.core.server.session.optimisation;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Optional;

/**
 * Optimisation applied to use Janus indices in the following additional case:
 * <p>
 * <code>
 * g.V().outE().values(c).as(b).V().filter(__.properties(a).where(P.eq(b)));
 * </code>
 * <p>
 * In this instance, the vertex can be looked up directly in Janus, joining the {@code V().filter(..)}
 * steps together.
 *
 */
@SuppressWarnings("ComparableType")
public class JanusPreviousPropertyStepStrategy extends AbstractTraversalStrategy<ProviderOptimizationStrategy> implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 6888929702831948298L;

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {

        // Retrieve all graph (`V()`) steps - this is the step the strategy should replace
        List<GraphStep> graphSteps = TraversalHelper.getStepsOfAssignableClass(GraphStep.class, traversal);

        for (GraphStep graphStep : graphSteps) {
            // For each graph step, confirm it follows this pattern:
            // `V().filter(__.properties(a).where(P.eq(b)))`

            if (!(graphStep.getNextStep() instanceof TraversalFilterStep)) continue;
            TraversalFilterStep<Vertex> filterStep = (TraversalFilterStep<Vertex>) graphStep.getNextStep();

            // Retrieve the filter steps e.g. `__.properties(a).where(P.eq(b))`
            List<Step> steps = stepsFromFilterStep(filterStep);

            if (steps.size() < 2) continue;
            Step propertiesStep = steps.get(0); // This is `properties(a)`
            Step whereStep = steps.get(1);      // This is `filter(__.where(P.eq(b)))`

            // Get the property key `a`
            if (!(propertiesStep instanceof PropertiesStep)) continue;
            Optional<String> propertyKey = propertyFromPropertiesStep((PropertiesStep<Vertex>) propertiesStep);
            if (!propertyKey.isPresent()) continue;

            // Get the step label `b`
            if (!(whereStep instanceof WherePredicateStep)) continue;
            Optional<String> label = labelFromWhereEqPredicate((WherePredicateStep<Vertex>) whereStep);
            if (!label.isPresent()) continue;

            executeStrategy(traversal, graphStep, filterStep, propertyKey.get(), label.get());
        }
    }

    private List<Step> stepsFromFilterStep(TraversalFilterStep<Vertex> filterStep) {
        // TraversalFilterStep always has exactly one child, so this is safe
        return filterStep.getLocalChildren().get(0).getSteps();
    }

    private Optional<String> propertyFromPropertiesStep(PropertiesStep<Vertex> propertiesStep) {
        String[] propertyKeys = propertiesStep.getPropertyKeys();
        if (propertyKeys.length != 1) return Optional.empty();
        return Optional.of(propertyKeys[0]);
    }

    private Optional<String> labelFromWhereEqPredicate(WherePredicateStep<Vertex> whereStep) {
        Optional<P<?>> optionalPredicate = whereStep.getPredicate();

        return optionalPredicate.flatMap(predicate -> {
            if (!predicate.getBiPredicate().equals(Compare.eq)) return Optional.empty();
            return Optional.of((String) predicate.getValue());
        });
    }

    /**
     * Replace the {@code graphStep} and {@code filterStep} with a new JanusPreviousPropertyStep in the given
     * {@code traversal}.
     */
    private void executeStrategy(
            Traversal.Admin<?, ?> traversal, GraphStep<?, ?> graphStep, TraversalFilterStep<Vertex> filterStep,
            String propertyKey, String label) {

        JanusPreviousPropertyStep newStep = new JanusPreviousPropertyStep(traversal, propertyKey, label);
        traversal.removeStep(filterStep);
        TraversalHelper.replaceStep(graphStep, newStep, traversal);
    }
}
