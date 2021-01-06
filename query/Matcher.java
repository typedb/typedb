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

package grakn.core.query;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.concept.thing.Attribute;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.Reasoner;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.variable.Reference;
import graql.lang.query.GraqlMatch;
import graql.lang.query.builder.Sortable;

import java.util.Collections;
import java.util.Comparator;
import java.util.OptionalDouble;
import java.util.PriorityQueue;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.AGGREGATE_ATTRIBUTE_NOT_NUMBER;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.exception.ErrorMessage.ThingRead.SORT_ATTRIBUTE_NOT_COMPARABLE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.SORT_VARIABLE_NOT_ATTRIBUTE;
import static grakn.core.common.iterator.Iterators.iterate;
import static java.lang.Math.sqrt;

public class Matcher {

    private final Reasoner reasoner;
    private final GraqlMatch query;
    private final Disjunction disjunction;
    private final Options.Query options;

    public Matcher(Reasoner reasoner, GraqlMatch query, Options.Query options) {
        this.reasoner = reasoner;
        this.query = query;
        this.disjunction = Disjunction.create(query.conjunction().normalise());
        this.options = options;
    }

    public static Matcher create(Reasoner reasoner, GraqlMatch query, Options.Query options) {
        return new Matcher(reasoner, query, options);
    }

    public static Matcher.Aggregator create(Reasoner reasoner, GraqlMatch.Aggregate query, Options.Query options) {
        return new Matcher(reasoner, query.query(), options).new Aggregator(query);
    }

    public static Matcher.Group create(Reasoner reasoner, GraqlMatch.Group query, Options.Query options) {
        return new Matcher(reasoner, query.query(), options).new Group(query);
    }

    public static Matcher.Group.Aggregator create(Reasoner reasoner, GraqlMatch.Group.Aggregate query, Options.Query options) {
        return new Matcher(reasoner, query.group().query(), options).new Group(query.group()).new Aggregator(query);
    }

    public ResourceIterator<ConceptMap> execute(boolean isParallel) {
        return filter(reasoner.execute(disjunction, isParallel));
    }

    private ResourceIterator<ConceptMap> filter(ResourceIterator<ConceptMap> answers) {
        if (!query.filter().isEmpty()) {
            Set<Reference.Name> vars = iterate(query.filter()).map(f -> f.reference().asName()).toSet();
            answers = answers.map(a -> a.filter(vars)).distinct();
        }
        if (query.sort().isPresent()) {
            answers = sort(answers, query.sort().get());
        }
        if (query.offset().isPresent()) {
            answers = answers.offset(query.offset().get());
        }
        if (query.limit().isPresent()) {
            answers = answers.limit(query.limit().get());
        }
        return answers;
    }

    private ResourceIterator<ConceptMap> sort(ResourceIterator<ConceptMap> answers, Sortable.Sorting sorting) {
        // TODO: Replace this temporary implementation of Graql Match Sort query
        Reference.Name var = sorting.var().reference().asName();
        Comparator<ConceptMap> comparator = (answer1, answer2) -> {
            Attribute att1, att2;
            try {
                att1 = answer1.get(var).asAttribute();
                att2 = answer2.get(var).asAttribute();
            } catch (GraknException e) {
                if (e.code().isPresent() || e.code().get().equals(INVALID_THING_CASTING.code())) {
                    throw GraknException.of(SORT_VARIABLE_NOT_ATTRIBUTE, var);
                } else {
                    throw e;
                }
            }

            if (!att1.getType().getValueType().comparables().contains(att2.getType().getValueType())) {
                throw GraknException.of(SORT_ATTRIBUTE_NOT_COMPARABLE, var);
            }
            if (att1.isString()) {
                return att1.asString().getValue().compareToIgnoreCase(att2.asString().getValue());
            } else if (att1.isBoolean()) {
                return att1.asBoolean().getValue().compareTo(att2.asBoolean().getValue());
            } else if (att1.isLong() && att2.isLong()) {
                return att1.asLong().getValue().compareTo(att2.asLong().getValue());
            } else if (att1.isDouble() || att2.isDouble()) {
                Double double1 = att1.isLong() ? att1.asLong().getValue() : att1.asDouble().getValue();
                Double double2 = att2.isLong() ? att2.asLong().getValue() : att2.asDouble().getValue();
                return double1.compareTo(double2);
            } else if (att1.isDateTime()) {
                return (att1.asDateTime().getValue()).compareTo(att2.asDateTime().getValue());
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        };
        comparator = (sorting.order() == GraqlArg.Order.DESC) ? comparator.reversed() : comparator;
        return iterate(answers.stream().sorted(comparator).iterator());
    }

    public class Aggregator {

        private final GraqlMatch.Aggregate query;

        public Aggregator(GraqlMatch.Aggregate query) {
            this.query = query;
        }

        public Numeric execute(boolean isParallel) {
            ResourceIterator<ConceptMap> answers = Matcher.this.execute(isParallel);
            switch (query.method()) {
                case COUNT:
                    return count(answers);
                case MAX:
                    return max(answers);
                case MEAN:
                    return mean(answers);
                case MEDIAN:
                    return median(answers);
                case MIN:
                    return min(answers);
                case STD:
                    return std(answers);
                case SUM:
                    return sum(answers);
                default:
                    throw GraknException.of(UNRECOGNISED_VALUE);
            }
        }

        private Numeric getValue(ConceptMap answer) {
            Attribute attribute = answer.get(query.var()).asAttribute();
            if (attribute.isLong()) return Numeric.ofLong(attribute.asLong().getValue());
            else if (attribute.isDouble()) return Numeric.ofDouble(attribute.asDouble().getValue());
            else throw GraknException.of(AGGREGATE_ATTRIBUTE_NOT_NUMBER, query.var());
        }

        private Numeric count(ResourceIterator<ConceptMap> answers) {
            return Numeric.ofLong(answers.count());
        }

        private Numeric max(ResourceIterator<ConceptMap> answers) {
            return answers.map(this::getValue).stream().max(new SortComparator()).orElse(Numeric.ofNaN());
        }

        private Numeric mean(ResourceIterator<ConceptMap> answers) {
            OptionalDouble mean = answers.stream().mapToDouble(a -> getValue(a).asNumber().doubleValue()).average();
            if (mean.isPresent()) return Numeric.ofDouble(mean.getAsDouble());
            else return Numeric.ofNaN();
        }

        private Numeric median(ResourceIterator<ConceptMap> answers) {
            MedianFinder medianFinder = new MedianFinder();
            answers.map(this::getValue).forEachRemaining(medianFinder::addNum);
            return medianFinder.findMedian();
        }

        private Numeric min(ResourceIterator<ConceptMap> answers) {
            return answers.map(this::getValue).stream().min(new SortComparator()).orElse(Numeric.ofNaN());
        }

        /**
         * Online algorithm to calculate unbiased sample standard deviation
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
         * // TODO: We may find a faster algorithm that does not cost so much as the division in the loop
         */
        private Numeric std(ResourceIterator<ConceptMap> answers) {
            ResourceIterator<Double> values = answers.map(result -> getValue(result).asNumber().doubleValue());
            long n = 0;
            double mean = 0d, M2 = 0d;

            while (values.hasNext()) {
                double x = values.next();
                n += 1;
                double delta = x - mean;
                mean += delta / (double) n;
                double delta2 = x - mean;
                M2 += delta * delta2;
            }

            if (n < 2) return Numeric.ofNaN();
            else return Numeric.ofDouble(sqrt(M2 / (double) (n - 1)));
        }

        private Numeric sum(ResourceIterator<ConceptMap> answers) {
            // initial value is set to null so that we can return null if there is no Answers to consume
            return answers.map(this::getValue).stream().reduce(Numeric.ofNaN(), this::addNumbers);
        }

        private Numeric addNumbers(Numeric x, Numeric y) {
            // if this method is called, then there is at least one number to apply SumAggregate to, thus we set x back to 0
            if (x.isNaN()) x = Numeric.ofLong(0);

            // This method is necessary because Number doesn't support '+' because java!
            if (x.isLong() && y.isLong()) return Numeric.ofLong(x.asLong() + y.asLong());
            else if (x.isLong()) return Numeric.ofDouble(x.asLong() + y.asDouble());
            else if (y.isLong()) return Numeric.ofDouble(x.asDouble() + y.asLong());
            else return Numeric.ofDouble(x.asDouble() + y.asDouble());
        }

        private class MedianFinder {

            PriorityQueue<Numeric> maxHeap; //lower half
            PriorityQueue<Numeric> minHeap; //higher half

            MedianFinder() {
                maxHeap = new PriorityQueue<>(Collections.reverseOrder());
                minHeap = new PriorityQueue<>();
            }

            // Adds a number into the data structure.
            void addNum(Numeric numeric) {
                maxHeap.offer(numeric);
                minHeap.offer(maxHeap.poll());

                if (maxHeap.size() < minHeap.size()) {
                    maxHeap.offer(minHeap.poll());
                }
            }

            // Returns the median of current data stream
            Numeric findMedian() {
                if (maxHeap.isEmpty() && minHeap.isEmpty()) {
                    return Numeric.ofNaN();
                } else if (maxHeap.size() == minHeap.size()) {
                    return Numeric.ofDouble(addNumbers(maxHeap.peek(), minHeap.peek()).asNumber().doubleValue() / 2);
                } else if (maxHeap.peek() == null) {
                    return Numeric.ofNaN();
                } else {
                    return maxHeap.peek();
                }
            }
        }

        public class SortComparator implements Comparator<Numeric> {

            @Override
            public int compare(Numeric a, Numeric b) {
                return Double.compare(a.asNumber().doubleValue(), b.asNumber().doubleValue());
            }
        }
    }

    public class Group {

        private final GraqlMatch.Group query;

        public Group(GraqlMatch.Group query) {
            this.query = query;
        }

        public ResourceIterator<ConceptMapGroup> execute(boolean isParallel) {
            throw GraknException.of(UNIMPLEMENTED);
        }

        public class Aggregator {

            private final GraqlMatch.Group.Aggregate query;

            public Aggregator(GraqlMatch.Group.Aggregate query) {
                this.query = query;
            }

            public ResourceIterator<NumericGroup> execute(boolean isParallel) {
                throw GraknException.of(UNIMPLEMENTED);
            }
        }
    }
}
