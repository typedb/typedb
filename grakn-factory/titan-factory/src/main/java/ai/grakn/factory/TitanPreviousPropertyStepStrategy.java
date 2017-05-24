/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.factory;

import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.TitanGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Optional;

/**
 * Optimisation applied to use Titan indices in the following additional case:
 * <p>
 * <code>
 * g.V().outE().values("foo").as("x").V().has("bar", __.where(P.eq("x")));
 * </code>
 * <p>
 * In this instance, the vertex can be looked up directly in Titan, joining the {@code V().has(..)} steps together.
 *
 * @author Felix Chapman
 */
public class TitanPreviousPropertyStepStrategy extends AbstractTraversalStrategy<ProviderOptimizationStrategy> implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 6888929702831948298L;

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<TitanGraphStep> graphSteps = TraversalHelper.getStepsOfClass(TitanGraphStep.class, traversal);

        for (TitanGraphStep graphStep : graphSteps) {

            if (!(graphStep.getNextStep() instanceof TraversalFilterStep)) continue;
            TraversalFilterStep<Vertex> filterStep = (TraversalFilterStep<Vertex>) graphStep.getNextStep();

            List<Step> steps = stepsFromFilterStep(filterStep);

            if (steps.size() < 2) continue;
            Step propertiesStep = steps.get(0);
            Step whereStep = steps.get(1);

            if (!(propertiesStep instanceof PropertiesStep)) continue;
            Optional<String> propertyKey = propertyFromPropertiesStep((PropertiesStep<Vertex>) propertiesStep);
            if (!propertyKey.isPresent()) continue;

            if (!(whereStep instanceof WherePredicateStep)) continue;
            Optional<String> label = eqPredicateFromWherePredicate((WherePredicateStep<Vertex>) whereStep);
            if (!label.isPresent()) continue;

            executeStrategy(traversal, graphStep, filterStep, propertyKey.get(), label.get());
        }
    }

    private List<Step> stepsFromFilterStep(TraversalFilterStep<Vertex> filterStep) {
        // TraversalFilterStep always has exactly one child
        return filterStep.getLocalChildren().get(0).getSteps();
    }

    private Optional<String> propertyFromPropertiesStep(PropertiesStep<Vertex> propertiesStep) {
        String[] propertyKeys = propertiesStep.getPropertyKeys();
        if (propertyKeys.length != 1) return Optional.empty();
        return Optional.of(propertyKeys[0]);
    }

    private <T> Optional<T> eqPredicateFromWherePredicate(WherePredicateStep<Vertex> whereStep) {
        Optional<P<?>> optionalPredicate = whereStep.getPredicate();

        return optionalPredicate.flatMap(predicate -> {
            if (!predicate.getBiPredicate().equals(Compare.eq)) return Optional.empty();
            return Optional.of((T) predicate.getValue());
        });
    }

    private void executeStrategy(
            Traversal.Admin<?, ?> traversal, TitanGraphStep<?, ?> graphStep, TraversalFilterStep<Vertex> filterStep,
            String propertyKey, String label) {

        TitanPreviousPropertyStep newStep = new TitanPreviousPropertyStep(traversal, propertyKey, label);
        traversal.removeStep(filterStep);
        TraversalHelper.replaceStep(graphStep, newStep, traversal);
    }
}
