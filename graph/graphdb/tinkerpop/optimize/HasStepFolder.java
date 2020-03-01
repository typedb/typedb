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

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.QueryUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public interface HasStepFolder<S, E> extends Step<S, E> {

    void addAll(Iterable<HasContainer> hasContainers);

    List<HasContainer> addLocalAll(Iterable<HasContainer> hasContainers);

    void orderBy(String key, Order order);

    void localOrderBy(List<HasContainer> hasContainers, String key, Order order);

    void setLimit(int low, int high);

    void setLocalLimit(List<HasContainer> hasContainers, int low, int high);

    int getLowLimit();

    int getLocalLowLimit(List<HasContainer> hasContainers);

    int getHighLimit();

    int getLocalHighLimit(List<HasContainer> hasContainers);

    static boolean validJanusGraphHas(HasContainer has) {
        if (has.getPredicate() instanceof ConnectiveP) {
            List<? extends P<?>> predicates = ((ConnectiveP<?>) has.getPredicate()).getPredicates();
            return predicates.stream().allMatch(p -> validJanusGraphHas(new HasContainer(has.getKey(), p)));
        } else {
            return JanusGraphPredicate.Converter.supports(has.getBiPredicate());
        }
    }

    static boolean validJanusGraphHas(Iterable<HasContainer> has) {
        for (HasContainer h : has) {
            if (!validJanusGraphHas(h)) return false;
        }
        return true;
    }

    static boolean validJanusGraphOrder(OrderGlobalStep orderGlobalStep, Traversal rootTraversal, boolean isVertexOrder) {
        List<Pair<Traversal.Admin, Object>> comparators = orderGlobalStep.getComparators();
        for (Pair<Traversal.Admin, Object> comp : comparators) {
            String key;
            if (comp.getValue0() instanceof ElementValueTraversal &&
                    comp.getValue1() instanceof Order) {
                key = ((ElementValueTraversal) comp.getValue0()).getPropertyKey();
            } else if (comp.getValue1() instanceof ElementValueComparator) {
                ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                if (!(evc.getValueComparator() instanceof Order)) return false;
                key = evc.getPropertyKey();
            } else {
                // do not fold comparators that include nested traversals that are not simple ElementValues
                return false;
            }
            JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(rootTraversal.asAdmin());
            PropertyKey pKey = tx.getPropertyKey(key);
            if (pKey == null
                    || !(Comparable.class.isAssignableFrom(pKey.dataType()))
                    || (isVertexOrder && pKey.cardinality() != Cardinality.SINGLE)) {
                return false;
            }
        }
        return true;
    }

    static void foldInIds(HasStepFolder janusgraphStep, Traversal.Admin<?, ?> traversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) currentStep;
                GraphStep graphStep = (GraphStep) janusgraphStep;
                // HasContainer collection that we get back is immutable so we keep track of which containers
                // need to be deleted after they've been folded into the JanusGraphStep and then remove them from their
                // step using HasContainer.removeHasContainer
                List<HasContainer> removableHasContainers = new ArrayList<>();
                Set<String> stepLabels = currentStep.getLabels();
                hasContainerHolder.getHasContainers().forEach(hasContainer -> {
                    if (GraphStep.processHasContainerIds(graphStep, hasContainer)) {
                        stepLabels.forEach(janusgraphStep::addLabel);
                        // this has container is no longer needed because its ids will be folded into the JanusGraphStep
                        removableHasContainers.add(hasContainer);
                    }
                });

                if (!removableHasContainers.isEmpty()) {
                    removableHasContainers.forEach(hasContainerHolder::removeHasContainer);
                }
                // if all has containers have been removed, the current step can be removed
                if (hasContainerHolder.getHasContainers().isEmpty()) {
                    traversal.removeStep(currentStep);
                }
//            } else if (currentStep instanceof IdentityStep) {
//                // do nothing, has no impact
//            } else if (currentStep instanceof NoOpBarrierStep) {
//                // do nothing, has no impact
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void foldInHasContainer(HasStepFolder janusgraphStep, Traversal.Admin<?, ?> traversal, Traversal<?, ?> rootTraversal) {
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof OrStep && janusgraphStep instanceof JanusGraphStep) {
                for (Traversal.Admin<?, ?> child : ((OrStep<?>) currentStep).getLocalChildren()) {
                    if (!validFoldInHasContainer(child.getStartStep(), false)) {
                        return;
                    }
                }
                ((OrStep<?>) currentStep).getLocalChildren().forEach(t -> localFoldInHasContainer(janusgraphStep, t.getStartStep(), t, rootTraversal));
                traversal.removeStep(currentStep);
            } else if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers().stream().map(c -> JanusGraphPredicate.Converter.convert(c)).collect(Collectors.toList());
                if (validFoldInHasContainer(currentStep, true)) {
                    janusgraphStep.addAll(containers);
                    currentStep.getLabels().forEach(janusgraphStep::addLabel);
                    traversal.removeStep(currentStep);
                }
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep) && !(currentStep instanceof HasContainerHolder)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static void localFoldInHasContainer(HasStepFolder janusgraphStep, Step<?, ?> tinkerpopStep, Traversal.Admin<?, ?> traversal, Traversal<?, ?> rootTraversal) {
        Step<?, ?> currentStep = tinkerpopStep;
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers().stream().map(c -> JanusGraphPredicate.Converter.convert(c)).collect(Collectors.toList());
                List<HasContainer> hasContainers = janusgraphStep.addLocalAll(containers);
                currentStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(currentStep);
                currentStep = foldInOrder(janusgraphStep, currentStep, traversal, rootTraversal, janusgraphStep instanceof JanusGraphStep && ((JanusGraphStep) janusgraphStep).returnsVertex(), hasContainers);
                foldInRange(janusgraphStep, currentStep, traversal, hasContainers);
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    static boolean validFoldInHasContainer(Step<?, ?> tinkerpopStep, boolean defaultValue) {
        Step<?, ?> currentStep = tinkerpopStep;
        Boolean toReturn = null;
        while (!(currentStep instanceof EmptyStep)) {
            if (currentStep instanceof HasContainerHolder) {
                Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
                toReturn = toReturn == null ? validJanusGraphHas(containers) : toReturn && validJanusGraphHas(containers);
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep) && !(currentStep instanceof RangeGlobalStep) && !(currentStep instanceof OrderGlobalStep)) {
                toReturn = toReturn != null && (toReturn && defaultValue);
                break;
            }
            currentStep = currentStep.getNextStep();
        }
        return Boolean.TRUE.equals(toReturn);
    }

    static Step<?, ?> foldInOrder(HasStepFolder janusgraphStep, Step<?, ?> tinkerpopStep, Traversal.Admin<?, ?> traversal,
                                  Traversal<?, ?> rootTraversal, boolean isVertexOrder, List<HasContainer> hasContainers) {
        Step<?, ?> currentStep = tinkerpopStep;
        OrderGlobalStep<?, ?> lastOrder = null;
        while (true) {
            if (currentStep instanceof OrderGlobalStep) {
                if (lastOrder != null) { //Previous orders are rendered irrelevant by next order (since re-ordered)
                    lastOrder.getLabels().forEach(janusgraphStep::addLabel);
                    traversal.removeStep(lastOrder);
                }
                lastOrder = (OrderGlobalStep) currentStep;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof HasStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        if (lastOrder != null && validJanusGraphOrder(lastOrder, rootTraversal, isVertexOrder)) {
            //Add orders to HasStepFolder
            for (Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>> comp : (List<Pair<Traversal.Admin<Object, Comparable>, Comparator<Object>>>) ((OrderGlobalStep) lastOrder).getComparators()) {
                String key;
                Order order;
                if (comp.getValue0() instanceof ElementValueTraversal) {
                    ElementValueTraversal evt = (ElementValueTraversal) comp.getValue0();
                    key = evt.getPropertyKey();
                    order = (Order) comp.getValue1();
                } else {
                    ElementValueComparator evc = (ElementValueComparator) comp.getValue1();
                    key = evc.getPropertyKey();
                    order = (Order) evc.getValueComparator();
                }
                if (hasContainers == null) {
                    janusgraphStep.orderBy(key, order);
                } else {
                    janusgraphStep.localOrderBy(hasContainers, key, order);
                }
            }
            lastOrder.getLabels().forEach(janusgraphStep::addLabel);
            traversal.removeStep(lastOrder);
        }
        return currentStep;
    }

    static List<HasContainer> splitAndP(List<HasContainer> hasContainers, Iterable<HasContainer> has) {
        has.forEach(hasContainer -> {
            if (hasContainer.getPredicate() instanceof AndP) {
                for (P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                    hasContainers.add(new HasContainer(hasContainer.getKey(), predicate));
                }
            } else {
                hasContainers.add(hasContainer);
            }
        });
        return hasContainers;
    }

    class OrderEntry {

        public final String key;
        public final Order order;

        public OrderEntry(String key, Order order) {
            this.key = key;
            this.order = order;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final OrderEntry that = (OrderEntry) o;

            if (key != null ? !key.equals(that.key) : that.key != null) return false;
            return order == that.order;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (order != null ? order.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "OrderEntry{" +
                    "key='" + key + '\'' +
                    ", order=" + order +
                    '}';
        }
    }

    static void foldInRange(HasStepFolder janusgraphStep, Step<?, ?> tinkerpopStep, Traversal.Admin<?, ?> traversal, List<HasContainer> hasContainers) {
        Step<?, ?> nextStep = tinkerpopStep instanceof IdentityStep ? JanusGraphTraversalUtil.getNextNonIdentityStep(tinkerpopStep) : tinkerpopStep;
        if (nextStep instanceof RangeGlobalStep) {
            RangeGlobalStep range = (RangeGlobalStep) nextStep;
            int low = 0;
            if (janusgraphStep instanceof JanusGraphStep) {
                low = QueryUtil.convertLimit(range.getLowRange());
                low = QueryUtil.mergeLowLimits(low, hasContainers == null ? janusgraphStep.getLowLimit() : janusgraphStep.getLocalLowLimit(hasContainers));
            }
            int high = QueryUtil.convertLimit(range.getHighRange());
            high = QueryUtil.mergeHighLimits(high, hasContainers == null ? janusgraphStep.getHighLimit() : janusgraphStep.getLocalHighLimit(hasContainers));
            if (hasContainers == null) {
                janusgraphStep.setLimit(low, high);
            } else {
                janusgraphStep.setLocalLimit(hasContainers, low, high);
            }
            if (janusgraphStep instanceof JanusGraphStep || range.getLowRange() == 0) { //Range can be removed since there is JanusGraphStep or no offset
                nextStep.getLabels().forEach(janusgraphStep::addLabel);
                traversal.removeStep(nextStep);
            }
        }
    }

    /**
     * @param janusgraphStep The step to test
     * @return True if there are 'has' steps following this step and no subsequent range limit step
     */
    static boolean foldableHasContainerNoLimit(FlatMapStep<?, ?> janusgraphStep) {
        boolean foldableHasContainerNoLimit = false;
        Step<?, ?> currentStep = janusgraphStep.getNextStep();
        while (true) {
            if (currentStep instanceof OrStep) {
                for (Traversal.Admin<?, ?> child : ((OrStep<?>) currentStep).getLocalChildren()) {
                    if (!validFoldInHasContainer(child.getStartStep(), false)) {
                        return false;
                    }
                }
                foldableHasContainerNoLimit = true;
            } else if (currentStep instanceof HasContainerHolder) {
                if (validFoldInHasContainer(currentStep, true)) {
                    foldableHasContainerNoLimit = true;
                }
            } else if (currentStep instanceof RangeGlobalStep) {
                return false;
            } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep)) {
                break;
            }
            currentStep = currentStep.getNextStep();
        }

        return foldableHasContainerNoLimit;
    }
}
