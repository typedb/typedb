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

package grakn.core.query;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Context;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.concept.thing.Attribute;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.Reasoner;
import grakn.core.traversal.common.Identifier;
import graql.lang.common.GraqlArg;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;
import graql.lang.pattern.variable.UnboundVariable;
import graql.lang.query.GraqlMatch;
import graql.lang.query.builder.Sortable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static grakn.common.collection.Collections.set;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.AGGREGATE_ATTRIBUTE_NOT_NUMBER;
import static grakn.core.common.exception.ErrorMessage.ThingRead.INVALID_THING_CASTING;
import static grakn.core.common.exception.ErrorMessage.ThingRead.SORT_ATTRIBUTE_NOT_COMPARABLE;
import static grakn.core.common.exception.ErrorMessage.ThingRead.SORT_VARIABLE_NOT_ATTRIBUTE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;
import static grakn.core.query.Matcher.Aggregator.aggregator;
import static java.lang.Math.sqrt;
import static java.util.stream.Collectors.groupingBy;

public class Matcher {

    private final Reasoner reasoner;
    private final GraqlMatch query;
    private final Disjunction disjunction;
    private final List<Identifier.Variable.Name> filter;
    private final Context.Query context;

    public Matcher(Reasoner reasoner, GraqlMatch query, @Nullable Context.Query context) {
        this.reasoner = reasoner;
        this.query = query;
        this.disjunction = Disjunction.create(query.conjunction().normalise());
        this.filter = iterate(query.filter()).map(v -> Identifier.Variable.of(v.reference().asName())).toList();
        this.context = context;
        if (context != null) this.context.producer(INCREMENTAL);
    }

    public static Matcher create(Reasoner reasoner, GraqlMatch query) {
        return create(reasoner, query, null);
    }

    public static Matcher create(Reasoner reasoner, GraqlMatch query, @Nullable Context.Query context) {
        return new Matcher(reasoner, query, context);
    }

    public static Matcher.Aggregator create(Reasoner reasoner, GraqlMatch.Aggregate query, Context.Query context) {
        Matcher matcher = new Matcher(reasoner, query.match(), context);
        return new Aggregator(matcher, query);
    }

    public static Matcher.Group create(Reasoner reasoner, GraqlMatch.Group query, Context.Query context) {
        Matcher matcher = new Matcher(reasoner, query.match(), context);
        return new Group(matcher, query);
    }

    public static Matcher.Group.Aggregator create(Reasoner reasoner, GraqlMatch.Group.Aggregate query, Context.Query context) {
        Matcher matcher = new Matcher(reasoner, query.group().match(), context);
        Group group = new Group(matcher, query.group());
        return new Group.Aggregator(group, query);
    }

    public ResourceIterator<ConceptMap> execute() {
        assert context != null;
        return execute(context);
    }

    ResourceIterator<ConceptMap> execute(Context.Query context) {
        ResourceIterator<ConceptMap> answers = reasoner.execute(disjunction, filter, context);
        if (query.sort().isPresent()) answers = sort(answers, query.sort().get());
        if (query.offset().isPresent()) answers = answers.offset(query.offset().get());
        if (query.limit().isPresent()) answers = answers.limit(query.limit().get());
        return answers;
    }

    private ResourceIterator<ConceptMap> sort(ResourceIterator<ConceptMap> answers, Sortable.Sorting sorting) {
        // TODO: Replace this temporary implementation of Graql Match Sort query with a native sorting traversal
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

    public static class Aggregator {

        private final Matcher matcher;
        private final GraqlMatch.Aggregate query;

        public Aggregator(Matcher matcher, GraqlMatch.Aggregate query) {
            this.matcher = matcher;
            this.query = query;
        }

        public Numeric execute() {
            ResourceIterator<ConceptMap> answers = matcher.execute();
            GraqlToken.Aggregate.Method method = query.method();
            UnboundVariable var = query.var();
            return aggregate(answers, method, var);
        }

        static Numeric aggregate(ResourceIterator<ConceptMap> answers,
                                 GraqlToken.Aggregate.Method method, UnboundVariable var) {
            return answers.stream().collect(aggregator(method, var));
        }

        static Collector<ConceptMap, ?, Numeric> aggregator(GraqlToken.Aggregate.Method method, UnboundVariable var) {
            Collector<ConceptMap, ?, Numeric> aggregator;
            switch (method) {
                case COUNT:
                    aggregator = count();
                    break;
                case MAX:
                    aggregator = max(var);
                    break;
                case MEAN:
                    aggregator = mean(var);
                    break;
                case MEDIAN:
                    aggregator = median(var);
                    break;
                case MIN:
                    aggregator = min(var);
                    break;
                case STD:
                    aggregator = std(var);
                    break;
                case SUM:
                    aggregator = sum(var);
                    break;
                default:
                    throw GraknException.of(UNRECOGNISED_VALUE);
            }
            return aggregator;
        }

        static Collector<ConceptMap, ?, Numeric> count() {
            return new Collector<ConceptMap, Accumulator<Long>, Numeric>() {

                @Override
                public Supplier<Accumulator<Long>> supplier() {
                    return () -> new Accumulator<>(0L, Long::sum);
                }

                @Override
                public BiConsumer<Accumulator<Long>, ConceptMap> accumulator() {
                    return (sum, answer) -> sum.accept(1L);
                }

                @Override
                public BinaryOperator<Accumulator<Long>> combiner() {
                    return (sum1, sum2) -> {
                        sum1.accept(sum2.value);
                        return sum1;
                    };
                }

                @Override
                public Function<Accumulator<Long>, Numeric> finisher() {
                    return sum -> Numeric.ofLong(sum.value);
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Numeric> max(UnboundVariable var) {
            return new Collector<ConceptMap, OptionalAccumulator<Numeric>, Numeric>() {

                @Override
                public Supplier<OptionalAccumulator<Numeric>> supplier() {
                    return () -> new OptionalAccumulator<>(BinaryOperator.maxBy(NumericComparator.natural()));
                }

                @Override
                public BiConsumer<OptionalAccumulator<Numeric>, ConceptMap> accumulator() {
                    return (max, answer) -> max.accept(numeric(answer, var));
                }

                @Override
                public BinaryOperator<OptionalAccumulator<Numeric>> combiner() {
                    return (max1, max2) -> {
                        if (max2.present) max1.accept(max2.value);
                        return max1;
                    };
                }

                @Override
                public Function<OptionalAccumulator<Numeric>, Numeric> finisher() {
                    return max -> {
                        if (max.present) return max.value;
                        else return Numeric.ofNaN();
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Numeric> mean(UnboundVariable var) {
            return new Collector<ConceptMap, Double[], Numeric>() {

                @Override
                public Supplier<Double[]> supplier() {
                    return () -> new Double[]{0.0, 0.0};
                }

                @Override
                public BiConsumer<Double[], ConceptMap> accumulator() {
                    return (acc, answer) -> {
                        acc[0] += numeric(answer, var).asNumber().doubleValue();
                        acc[1]++;
                    };
                }

                @Override
                public BinaryOperator<Double[]> combiner() {
                    return (acc1, acc2) -> {
                        acc1[0] += acc2[0];
                        acc1[1] += acc2[1];
                        return acc1;
                    };
                }

                @Override
                public Function<Double[], Numeric> finisher() {
                    return acc -> {
                        if (acc[1] == 0) return Numeric.ofNaN();
                        else return Numeric.ofDouble(acc[0] / acc[1]);
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Numeric> median(UnboundVariable var) {
            return new Collector<ConceptMap, MedianCalculator, Numeric>() {

                @Override
                public Supplier<MedianCalculator> supplier() {
                    return MedianCalculator::new;
                }

                @Override
                public BiConsumer<MedianCalculator, ConceptMap> accumulator() {
                    return (medianFinder, answer) -> medianFinder.accumulate(numeric(answer, var));
                }

                @Override
                public BinaryOperator<MedianCalculator> combiner() {
                    return (t, u) -> { throw GraknException.of(ILLEGAL_OPERATION); };
                }

                @Override
                public Function<MedianCalculator, Numeric> finisher() {
                    return MedianCalculator::median;
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Numeric> min(UnboundVariable var) {
            return new Collector<ConceptMap, OptionalAccumulator<Numeric>, Numeric>() {

                @Override
                public Supplier<OptionalAccumulator<Numeric>> supplier() {
                    return () -> new OptionalAccumulator<>(BinaryOperator.minBy(NumericComparator.natural()));
                }

                @Override
                public BiConsumer<OptionalAccumulator<Numeric>, ConceptMap> accumulator() {
                    return (min, answer) -> min.accept(numeric(answer, var));
                }

                @Override
                public BinaryOperator<OptionalAccumulator<Numeric>> combiner() {
                    return (min1, min2) -> {
                        if (min2.present) min1.accept(min2.value);
                        return min1;
                    };
                }

                @Override
                public Function<OptionalAccumulator<Numeric>, Numeric> finisher() {
                    return min -> {
                        if (min.present) return min.value;
                        else return Numeric.ofNaN();
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Numeric> std(UnboundVariable var) {
            return new Collector<ConceptMap, STDCalculator, Numeric>() {

                @Override
                public Supplier<STDCalculator> supplier() {
                    return STDCalculator::new;
                }

                @Override
                public BiConsumer<STDCalculator, ConceptMap> accumulator() {
                    return (acc, answer) -> acc.accumulate(numeric(answer, var).asNumber().doubleValue());
                }

                @Override
                public BinaryOperator<STDCalculator> combiner() {
                    return (t, u) -> { throw GraknException.of(ILLEGAL_OPERATION); };
                }

                @Override
                public Function<STDCalculator, Numeric> finisher() {
                    return STDCalculator::std;
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Numeric> sum(UnboundVariable var) {
            return new Collector<ConceptMap, OptionalAccumulator<Numeric>, Numeric>() {

                @Override
                public Supplier<OptionalAccumulator<Numeric>> supplier() {
                    return () -> new OptionalAccumulator<>(Aggregator::sum);
                }

                @Override
                public BiConsumer<OptionalAccumulator<Numeric>, ConceptMap> accumulator() {
                    return (sum, answer) -> sum.accept(numeric(answer, var));
                }

                @Override
                public BinaryOperator<OptionalAccumulator<Numeric>> combiner() {
                    return (sum1, sum2) -> {
                        if (sum2.present) sum1.accept(sum2.value);
                        return sum1;
                    };
                }

                @Override
                public Function<OptionalAccumulator<Numeric>, Numeric> finisher() {
                    return sum -> {
                        if (sum.present) return sum.value;
                        else return Numeric.ofNaN();
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        private static Numeric numeric(ConceptMap answer, UnboundVariable var) {
            Attribute attribute = answer.get(var).asAttribute();
            if (attribute.isLong()) return Numeric.ofLong(attribute.asLong().getValue());
            else if (attribute.isDouble()) return Numeric.ofDouble(attribute.asDouble().getValue());
            else throw GraknException.of(AGGREGATE_ATTRIBUTE_NOT_NUMBER, var);
        }

        private static Numeric sum(Numeric x, Numeric y) {
            // This method is necessary because Number doesn't support '+' because java!
            if (x.isLong() && y.isLong()) return Numeric.ofLong(x.asLong() + y.asLong());
            else if (x.isLong()) return Numeric.ofDouble(x.asLong() + y.asDouble());
            else if (y.isLong()) return Numeric.ofDouble(x.asDouble() + y.asLong());
            else return Numeric.ofDouble(x.asDouble() + y.asDouble());
        }

        private static class MedianCalculator {

            PriorityQueue<Numeric> maxHeap; //lower half
            PriorityQueue<Numeric> minHeap; //higher half

            MedianCalculator() {
                maxHeap = new PriorityQueue<>(Collections.reverseOrder());
                minHeap = new PriorityQueue<>();
            }

            void accumulate(Numeric numeric) {
                maxHeap.offer(numeric);
                minHeap.offer(maxHeap.poll());

                if (maxHeap.size() < minHeap.size()) {
                    maxHeap.offer(minHeap.poll());
                }
            }

            Numeric median() {
                if (maxHeap.isEmpty() && minHeap.isEmpty()) {
                    return Numeric.ofNaN();
                } else if (maxHeap.size() == minHeap.size()) {
                    return Numeric.ofDouble(sum(maxHeap.peek(), minHeap.peek()).asNumber().doubleValue() / 2);
                } else if (maxHeap.peek() == null) {
                    return Numeric.ofNaN();
                } else {
                    return maxHeap.peek();
                }
            }
        }

        /**
         * Online algorithm to calculate unbiased sample standard deviation
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
         * // TODO: We may find a faster algorithm that does not cost so much as the division in the loop
         */
        private static class STDCalculator {
            long n = 0;
            double mean = 0d, M2 = 0d;

            void accumulate(double value) {
                n += 1;
                double delta = value - mean;
                mean += delta / (double) n;
                double delta2 = value - mean;
                M2 += delta * delta2;
            }

            Numeric std() {
                if (n < 2) return Numeric.ofNaN();
                else return Numeric.ofDouble(sqrt(M2 / (double) (n - 1)));
            }
        }

        private static class NumericComparator implements Comparator<Numeric> {

            static NumericComparator natural = new NumericComparator();

            public static Comparator<Numeric> natural() {
                return natural;
            }

            @Override
            public int compare(Numeric a, Numeric b) {
                return Double.compare(a.asNumber().doubleValue(), b.asNumber().doubleValue());
            }
        }

        private static class Accumulator<T> implements Consumer<T> {
            T value;
            private BinaryOperator<T> op;

            Accumulator(T init, BinaryOperator<T> op) {
                value = init;
                this.op = op;
            }

            @Override
            public void accept(T t) {
                value = op.apply(value, t);
            }
        }

        private static class OptionalAccumulator<T> implements Consumer<T> {
            T value = null;
            boolean present = false;

            private BinaryOperator<T> op;

            OptionalAccumulator(BinaryOperator<T> op) {
                this.op = op;
            }

            @Override
            public void accept(T t) {
                if (present) {
                    value = op.apply(value, t);
                } else {
                    value = t;
                    present = true;
                }
            }
        }
    }

    public static class Group {

        private final Matcher matcher;
        private final GraqlMatch.Group query;

        public Group(Matcher matcher, GraqlMatch.Group query) {
            this.matcher = matcher;
            this.query = query;
        }

        public ResourceIterator<ConceptMapGroup> execute() {
            // TODO: Replace this temporary implementation of Graql Match Group query with a native grouping traversal
            List<ConceptMapGroup> answerGroups = new ArrayList<>();
            matcher.execute().stream().collect(groupingBy(a -> a.get(query.var())))
                    .forEach((o, cm) -> answerGroups.add(new ConceptMapGroup(o, cm)));
            return iterate(answerGroups);
        }

        public static class Aggregator {

            private final Group group;
            private final GraqlMatch.Group.Aggregate query;

            public Aggregator(Group group, GraqlMatch.Group.Aggregate query) {
                this.group = group;
                this.query = query;
            }

            public ResourceIterator<NumericGroup> execute() {
                // TODO: Replace this temporary implementation of Graql Match Group query with a native grouping traversal
                List<NumericGroup> numericGroups = new ArrayList<>();
                group.matcher.execute().stream()
                        .collect(groupingBy(a -> a.get(query.group().var()), aggregator(query.method(), query.var())))
                        .forEach((o, n) -> numericGroups.add(new NumericGroup(o, n)));
                return iterate(numericGroups);
            }
        }
    }
}
