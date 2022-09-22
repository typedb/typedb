/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.query;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.Numeric;
import com.vaticle.typedb.core.concept.answer.NumericGroup;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;

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

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.AGGREGATE_ATTRIBUTE_NOT_NUMBER;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;
import static com.vaticle.typedb.core.query.Matcher.Aggregator.aggregator;
import static java.lang.Math.sqrt;
import static java.util.stream.Collectors.groupingBy;

public class Matcher {

    private final Reasoner reasoner;
    private final TypeQLMatch query;
    private final Disjunction disjunction;
    private final Context.Query context;

    public Matcher(Reasoner reasoner, TypeQLMatch query) {
        this(reasoner, query, null);
    }

    public Matcher(Reasoner reasoner, TypeQLMatch query, @Nullable Context.Query context) {
        this.reasoner = reasoner;
        this.query = query;
        this.disjunction = Disjunction.create(query.conjunction().normalise());
        this.context = context;
        if (context != null) {
            Either<Arguments.Query.Producer, Long> prodCtx;
            TypeQLMatch.Modifiers mods = query.modifiers();
            if (mods.sort().isPresent()) prodCtx = Either.first(EXHAUSTIVE); // TODO: remove this once sort is optimised
            else if (mods.limit().isPresent()) prodCtx = Either.second(mods.offset().orElse(0L) + mods.limit().get());
            else prodCtx = Either.first(INCREMENTAL);
            this.context.producer(prodCtx);
        }
    }

    public static Matcher create(Reasoner reasoner, TypeQLMatch query) {
        return new Matcher(reasoner, query);
    }

    public static Matcher create(Reasoner reasoner, TypeQLMatch query, Context.Query context) {
        return new Matcher(reasoner, query, context);
    }

    public static Matcher.Aggregator create(Reasoner reasoner, TypeQLMatch.Aggregate query, Context.Query context) {
        Matcher matcher = new Matcher(reasoner, query.match());
        return new Aggregator(matcher, query, context);
    }

    public static Matcher.Group create(Reasoner reasoner, TypeQLMatch.Group query, Context.Query context) {
        Matcher matcher = new Matcher(reasoner, query.match());
        return new Group(matcher, query, context);
    }

    public static Matcher.Group.Aggregator create(Reasoner reasoner, TypeQLMatch.Group.Aggregate query, Context.Query context) {
        Matcher matcher = new Matcher(reasoner, query.group().match());
        Group group = new Group(matcher, query.group(), context);
        return new Group.Aggregator(group, query);
    }

    public FunctionalIterator<? extends ConceptMap> execute() {
        assert context != null;
        return execute(context);
    }

    FunctionalIterator<? extends ConceptMap> execute(Context.Query context) {
        return reasoner.execute(disjunction, query.modifiers(), context);
    }

    public static class Aggregator {

        private final Matcher matcher;
        private final TypeQLMatch.Aggregate query;
        private final Context.Query context;

        public Aggregator(Matcher matcher, TypeQLMatch.Aggregate query, Context.Query context) {
            this.matcher = matcher;
            this.query = query;
            this.context = context;
            this.context.producer(Either.first(EXHAUSTIVE));
        }

        public Numeric execute() {
            FunctionalIterator<? extends ConceptMap> answers = matcher.execute(context);
            TypeQLToken.Aggregate.Method method = query.method();
            UnboundVariable var = query.var();
            return aggregate(answers, method, var);
        }

        static Numeric aggregate(FunctionalIterator<? extends ConceptMap> answers,
                                 TypeQLToken.Aggregate.Method method, UnboundVariable var) {
            return answers.stream().collect(aggregator(method, var));
        }

        static Collector<ConceptMap, ?, Numeric> aggregator(TypeQLToken.Aggregate.Method method, UnboundVariable var) {
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
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
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
                    return (t, u) -> {
                        throw TypeDBException.of(ILLEGAL_OPERATION);
                    };
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
                    return (t, u) -> {
                        throw TypeDBException.of(ILLEGAL_OPERATION);
                    };
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
            else throw TypeDBException.of(AGGREGATE_ATTRIBUTE_NOT_NUMBER, var);
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
        private final TypeQLMatch.Group query;
        private final Context.Query context;

        public Group(Matcher matcher, TypeQLMatch.Group query, Context.Query context) {
            this.matcher = matcher;
            this.query = query;
            this.context = context;
            this.context.producer(Either.first(EXHAUSTIVE));
        }

        public FunctionalIterator<ConceptMapGroup> execute() {
            // TODO: Replace this temporary implementation of TypeQL Match Group query with a native grouping traversal
            List<ConceptMapGroup> answerGroups = new ArrayList<>();
            matcher.execute(context).stream().collect(groupingBy(a -> a.get(query.var())))
                    .forEach((o, cm) -> answerGroups.add(new ConceptMapGroup(o, cm)));
            return iterate(answerGroups);
        }

        public static class Aggregator {

            private final Group group;
            private final TypeQLMatch.Group.Aggregate query;

            public Aggregator(Group group, TypeQLMatch.Group.Aggregate query) {
                this.group = group;
                this.query = query;
            }

            public FunctionalIterator<NumericGroup> execute() {
                // TODO: Replace this temporary implementation of TypeQL Match Group query with a native grouping traversal
                List<NumericGroup> numericGroups = new ArrayList<>();
                group.matcher.execute(group.context).stream()
                        .collect(groupingBy(a -> a.get(query.group().var()), aggregator(query.method(), query.var())))
                        .forEach((o, n) -> numericGroups.add(new NumericGroup(o, n)));
                return iterate(numericGroups);
            }
        }
    }
}
