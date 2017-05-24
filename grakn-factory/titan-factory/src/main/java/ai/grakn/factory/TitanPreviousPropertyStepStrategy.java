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
 * @author Felix Chapman
 */
public class TitanPreviousPropertyStepStrategy extends AbstractTraversalStrategy<ProviderOptimizationStrategy> implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 6888929702831948298L;

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<TitanGraphStep> graphSteps = TraversalHelper.getStepsOfClass(TitanGraphStep.class, traversal);

        for (TitanGraphStep graphStep : graphSteps) {

            Step nextStep = graphStep.getNextStep();

            if (!(nextStep instanceof TraversalFilterStep)) continue;

            TraversalFilterStep<Vertex> filterStep = (TraversalFilterStep<Vertex>) nextStep;

            List<Traversal.Admin<Vertex, ?>> children = filterStep.getLocalChildren();

            // TraversalFilterStep always has exactly one child
            Traversal.Admin<Vertex, ?> child = children.get(0);

            List<Step> steps = child.getSteps();

            if (steps.size() < 2) continue;

            Step step1 = steps.get(0);
            Step step2 = steps.get(1);

            if (!(step1 instanceof PropertiesStep)) continue;
            PropertiesStep<Vertex> propertiesStep = (PropertiesStep<Vertex>) step1;

            String[] propertyKeys = propertiesStep.getPropertyKeys();
            if (propertyKeys.length != 1) continue;
            String propertyKey = propertyKeys[0];

            if (!(step2 instanceof WherePredicateStep)) continue;
            WherePredicateStep<Vertex> whereStep = (WherePredicateStep<Vertex>) step2;

            Optional<P<?>> optionalPredicate = whereStep.getPredicate();
            if (!optionalPredicate.isPresent()) continue;
            P<?> predicate = optionalPredicate.get();

            if (!predicate.getBiPredicate().equals(Compare.eq)) continue;

            String label = (String) predicate.getValue();

            TitanPreviousPropertyStep newStep = new TitanPreviousPropertyStep(traversal, propertyKey, label);

            traversal.removeStep(filterStep);
            TraversalHelper.replaceStep(graphStep, newStep, traversal);
        }
    }
}
