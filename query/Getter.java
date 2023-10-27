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
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.ValueGroup;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.concept.value.impl.ValueImpl;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.Reasoner;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.common.TypeQLVariable;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLQuery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingRead.AGGREGATE_ATTRIBUTE_NOT_NUMBER;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.EXHAUSTIVE;
import static com.vaticle.typedb.core.query.Getter.Aggregator.aggregator;
import static java.lang.Math.sqrt;
import static java.util.stream.Collectors.groupingBy;

public class Getter {

    private final Reasoner reasoner;
    private final TypeQLQuery.MatchClause match;
    private final List<Identifier.Variable.Name> filter;
    private final TypeQLQuery.Modifiers modifiers;
    private final Disjunction disjunction;
    private final Context.Query context;

    public Getter(Reasoner reasoner, ConceptManager conceptMgr, TypeQLGet query) {
        this(reasoner, conceptMgr, query, null);
    }

    public Getter(Reasoner reasoner, ConceptManager conceptMgr, TypeQLGet query, Context.Query context) {
        this.reasoner = reasoner;
        this.match = query.match();
        this.modifiers = query.modifiers();
        this.filter = iterate(query.effectiveFilter()).map(v -> Identifier.Variable.of(v.reference().asName())).toList();
        if (this.modifiers.sort().isPresent()) {
            iterate(modifiers.sort().get().variables()).map(v -> Identifier.Variable.of(v.reference().asName()))
                    .forEachRemaining(filter::add);
        }
        this.disjunction = Disjunction.create(query.match().conjunction().normalise());
        this.context = context;
        iterate(disjunction.conjunctions())
                .flatMap(c -> iterate(c.variables())).flatMap(v -> iterate(v.constraints()))
                .filter(c -> c.isType() && c.asType().isLabel() && c.asType().asLabel().properLabel().scope().isPresent())
                // only validate labels that are used outside of relation constraints - this allows role type aliases in relations
                .filter(label -> label.owner().constraining().isEmpty() || iterate(label.owner().constraining()).anyMatch(c -> !(c.isThing() && c.asThing().isRelation())))
                .forEachRemaining(c -> conceptMgr.validateNotRoleTypeAlias(c.asType().asLabel().properLabel()));
    }

    public static Getter create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLGet query) {
        return new Getter(reasoner, conceptMgr, query);
    }

    public static Getter create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLGet query, Context.Query context) {
        return new Getter(reasoner, conceptMgr, query, context);
    }

    public static Getter.Aggregator create(Reasoner reasoner, ConceptManager conceptMgr,
                                           TypeQLGet.Aggregate query, Context.Query context) {
        Getter getter = new Getter(reasoner, conceptMgr, query.get());
        return new Aggregator(getter, query, conceptMgr, context);
    }

    public static Getter.Group create(Reasoner reasoner, ConceptManager conceptMgr, TypeQLGet.Group query, Context.Query context) {
        Getter getter = new Getter(reasoner, conceptMgr, query.get());
        return new Group(getter, query, context);
    }

    public static Getter.Group.Aggregator create(Reasoner reasoner, ConceptManager conceptMgr,
                                                 TypeQLGet.Group.Aggregate query, Context.Query context) {
        Getter getter = new Getter(reasoner, conceptMgr, query.group().get());
        Group group = new Group(getter, query.group(), context);
        return new Group.Aggregator(conceptMgr, group, query);
    }

    public Disjunction disjunction() {
        return disjunction;
    }

    public FunctionalIterator<? extends ConceptMap> execute() {
        return execute(context);
    }

    public FunctionalIterator<? extends ConceptMap> execute(Context.Query context) {
        return execute(new ConceptMap(), context);
    }

    FunctionalIterator<? extends ConceptMap> execute(ConceptMap bindings, Context.Query context) {
        return reasoner.execute(disjunction, filter, modifiers, context, bindings);
    }

    public static class Aggregator {

        private final Getter getter;
        private final ConceptManager conceptMgr;
        private final TypeQLGet.Aggregate query;
        private final Context.Query context;

        public Aggregator(Getter getter, TypeQLGet.Aggregate query, ConceptManager conceptMgr, Context.Query context) {
            this.getter = getter;
            this.query = query;
            this.conceptMgr = conceptMgr;
            this.context = context;
            this.context.producer(Either.first(EXHAUSTIVE));
        }

        public Getter getter() {
            return getter;
        }

        public Optional<Value<?>> execute() {
            return execute(new ConceptMap());
        }

        public Optional<Value<?>> execute(ConceptMap bindings) {
            FunctionalIterator<? extends ConceptMap> answers = getter.execute(bindings, context);
            TypeQLToken.Aggregate.Method method = query.method();
            TypeQLVariable var = query.var();
            return aggregate(conceptMgr, answers, method, var);
        }

        static Optional<Value<?>> aggregate(ConceptManager conceptMgr, FunctionalIterator<? extends ConceptMap> answers,
                                            TypeQLToken.Aggregate.Method method, TypeQLVariable var) {
            return answers.stream().collect(aggregator(conceptMgr, method, var));
        }

        static Value<?> getValue(ConceptManager conceptMgr, ConceptMap answer, TypeQLVariable var) {
            if (var.reference().isNameConcept()) {
                Attribute attribute = answer.get(var).asAttribute();
                if (attribute.isLong()) return createValue(conceptMgr, attribute.asLong().getValue());
                else if (attribute.isDouble()) return createValue(conceptMgr, attribute.asDouble().getValue());
                else throw TypeDBException.of(AGGREGATE_ATTRIBUTE_NOT_NUMBER, var);
            } else if (var.reference().isNameValue()) {
                return answer.get(var).asValue();
            } else throw TypeDBException.of(ILLEGAL_STATE);
        }

        static Value<Long> createValue(ConceptManager conceptMgr, long value) {
            try {
                return ValueImpl.of(conceptMgr, value);
            } catch (TypeDBCheckedException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        static Value<Double> createValue(ConceptManager conceptMgr, double value) {
            try {
                return ValueImpl.of(conceptMgr, value);
            } catch (TypeDBCheckedException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        private static Value<?> sumValue(ConceptManager conceptMgr, Value<?> x, Value<?> y) {
            // This method is necessary because Number doesn't support '+' because java!
            if (x.isLong() && y.isLong()) return createValue(conceptMgr, x.asLong().value() + y.asLong().value());
            else if (x.isLong()) return createValue(conceptMgr, x.asLong().value() + y.asDouble().value());
            else if (y.isLong()) return createValue(conceptMgr, x.asDouble().value() + y.asLong().value());
            else return createValue(conceptMgr, x.asDouble().value() + y.asDouble().value());
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> aggregator(ConceptManager conceptMgr, TypeQLToken.Aggregate.Method method, TypeQLVariable var) {
            Collector<ConceptMap, ?, Optional<Value<?>>> aggregator;
            switch (method) {
                case COUNT:
                    aggregator = count(conceptMgr);
                    break;
                case MAX:
                    aggregator = max(conceptMgr, var);
                    break;
                case MEAN:
                    aggregator = mean(conceptMgr, var);
                    break;
                case MEDIAN:
                    aggregator = median(conceptMgr, var);
                    break;
                case MIN:
                    aggregator = min(conceptMgr, var);
                    break;
                case STD:
                    aggregator = std(conceptMgr, var);
                    break;
                case SUM:
                    aggregator = sum(conceptMgr, var);
                    break;
                default:
                    throw TypeDBException.of(UNRECOGNISED_VALUE);
            }
            return aggregator;
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> count(ConceptManager conceptMgr) {
            return new Collector<ConceptMap, Accumulator<Long>, Optional<Value<?>>>() {

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
                public Function<Accumulator<Long>, Optional<Value<?>>> finisher() {
                    return sum -> Optional.of(createValue(conceptMgr, sum.value));
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> max(ConceptManager conceptMgr, TypeQLVariable var) {
            return new Collector<ConceptMap, OptionalAccumulator<Value<?>>, Optional<Value<?>>>() {

                @Override
                public Supplier<OptionalAccumulator<Value<?>>> supplier() {
                    return () -> new OptionalAccumulator<>(BinaryOperator.maxBy(NumericComparator.natural()));
                }

                @Override
                public BiConsumer<OptionalAccumulator<Value<?>>, ConceptMap> accumulator() {
                    return (max, answer) -> max.accept(getValue(conceptMgr, answer, var));
                }

                @Override
                public BinaryOperator<OptionalAccumulator<Value<?>>> combiner() {
                    return (max1, max2) -> {
                        if (max2.present) max1.accept(max2.value);
                        return max1;
                    };
                }

                @Override
                public Function<OptionalAccumulator<Value<?>>, Optional<Value<?>>> finisher() {
                    return max -> {
                        if (max.present) return Optional.of(max.value);
                        else return Optional.empty();
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> mean(ConceptManager conceptMgr, TypeQLVariable var) {
            return new Collector<ConceptMap, Double[], Optional<Value<?>>>() {

                @Override
                public Supplier<Double[]> supplier() {
                    return () -> new Double[]{0.0, 0.0};
                }

                @Override
                public BiConsumer<Double[], ConceptMap> accumulator() {
                    return (acc, answer) -> {
                        Value<?> value = getValue(conceptMgr, answer, var);
                        if (value.isDouble()) acc[0] += value.asDouble().value();
                        else if (value.isLong()) acc[0] += value.asLong().value();
                        else throw TypeDBException.of(ILLEGAL_STATE);
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
                public Function<Double[], Optional<Value<?>>> finisher() {
                    return acc -> {
                        if (acc[1] == 0) return Optional.empty();
                        else return Optional.of(createValue(conceptMgr, acc[0] / acc[1]));
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> median(ConceptManager conceptMgr, TypeQLVariable var) {
            return new Collector<ConceptMap, MedianCalculator, Optional<Value<?>>>() {

                @Override
                public Supplier<MedianCalculator> supplier() {
                    return MedianCalculator::new;
                }

                @Override
                public BiConsumer<MedianCalculator, ConceptMap> accumulator() {
                    return (medianFinder, answer) -> medianFinder.accumulate(getValue(conceptMgr, answer, var));
                }

                @Override
                public BinaryOperator<MedianCalculator> combiner() {
                    return (t, u) -> {
                        throw TypeDBException.of(ILLEGAL_OPERATION);
                    };
                }

                @Override
                public Function<MedianCalculator, Optional<Value<?>>> finisher() {
                    return medianCalculator -> medianCalculator.median(conceptMgr);
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> min(ConceptManager conceptMgr, TypeQLVariable var) {
            return new Collector<ConceptMap, OptionalAccumulator<Value<?>>, Optional<Value<?>>>() {

                @Override
                public Supplier<OptionalAccumulator<Value<?>>> supplier() {
                    return () -> new OptionalAccumulator<>(BinaryOperator.minBy(NumericComparator.natural()));
                }

                @Override
                public BiConsumer<OptionalAccumulator<Value<?>>, ConceptMap> accumulator() {
                    return (min, answer) -> min.accept(getValue(conceptMgr, answer, var));
                }

                @Override
                public BinaryOperator<OptionalAccumulator<Value<?>>> combiner() {
                    return (min1, min2) -> {
                        if (min2.present) min1.accept(min2.value);
                        return min1;
                    };
                }

                @Override
                public Function<OptionalAccumulator<Value<?>>, Optional<Value<?>>> finisher() {
                    return min -> {
                        if (min.present) return Optional.of(min.value);
                        else return Optional.empty();
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> std(ConceptManager conceptMgr, TypeQLVariable var) {
            return new Collector<ConceptMap, STDCalculator, Optional<Value<?>>>() {

                @Override
                public Supplier<STDCalculator> supplier() {
                    return () -> new STDCalculator(conceptMgr);
                }

                @Override
                public BiConsumer<STDCalculator, ConceptMap> accumulator() {
                    return (acc, answer) -> {
                        Value<?> value = getValue(conceptMgr, answer, var);
                        if (value.isDouble()) acc.accumulate(value.asDouble().value());
                        else if (value.isLong()) acc.accumulate(value.asLong().value());
                        else throw TypeDBException.of(ILLEGAL_STATE);
                    };
                }

                @Override
                public BinaryOperator<STDCalculator> combiner() {
                    return (t, u) -> {
                        throw TypeDBException.of(ILLEGAL_OPERATION);
                    };
                }

                @Override
                public Function<STDCalculator, Optional<Value<?>>> finisher() {
                    return STDCalculator::std;
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        static Collector<ConceptMap, ?, Optional<Value<?>>> sum(ConceptManager conceptMgr, TypeQLVariable var) {
            return new Collector<ConceptMap, OptionalAccumulator<Value<?>>, Optional<Value<?>>>() {

                @Override
                public Supplier<OptionalAccumulator<Value<?>>> supplier() {
                    return () -> new OptionalAccumulator<>((a, b) -> sumValue(conceptMgr, a, b));
                }

                @Override
                public BiConsumer<OptionalAccumulator<Value<?>>, ConceptMap> accumulator() {
                    return (sum, answer) -> sum.accept(getValue(conceptMgr, answer, var));
                }

                @Override
                public BinaryOperator<OptionalAccumulator<Value<?>>> combiner() {
                    return (sum1, sum2) -> {
                        if (sum2.present) sum1.accept(sum2.value);
                        return sum1;
                    };
                }

                @Override
                public Function<OptionalAccumulator<Value<?>>, Optional<Value<?>>> finisher() {
                    return sum -> {
                        if (sum.present) return Optional.of(sum.value);
                        else return Optional.empty();
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return set();
                }
            };
        }

        private static class MedianCalculator {

            PriorityQueue<Value<?>> maxHeap; //lower half
            PriorityQueue<Value<?>> minHeap; //higher half

            MedianCalculator() {
                maxHeap = new PriorityQueue<>(NumericComparator.natural().reversed());
                minHeap = new PriorityQueue<>(NumericComparator.natural());
            }

            void accumulate(Value<?> numeric) {
                maxHeap.offer(numeric);
                minHeap.offer(maxHeap.poll());

                if (maxHeap.size() < minHeap.size()) {
                    maxHeap.offer(minHeap.poll());
                }
            }

            Optional<Value<?>> median(ConceptManager conceptMgr) {
                if (maxHeap.isEmpty() && minHeap.isEmpty()) {
                    return Optional.empty();
                } else if (maxHeap.size() == minHeap.size()) {
                    Value<?> sum = sumValue(conceptMgr, maxHeap.peek(), minHeap.peek());
                    if (sum.isDouble()) return Optional.of(createValue(conceptMgr, sum.asDouble().value() / 2));
                    else if (sum.isLong())
                        return Optional.of(createValue(conceptMgr, (double) sum.asLong().value() / 2));
                    else throw TypeDBException.of(ILLEGAL_STATE);
                } else if (maxHeap.peek() == null) {
                    return Optional.empty();
                } else {
                    return Optional.of(maxHeap.peek());
                }
            }
        }

        /**
         * Online algorithm to calculate unbiased sample standard deviation
         * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
         * // TODO: We may find a faster algorithm that does not cost so much as the division in the loop
         */
        private static class STDCalculator {
            private final ConceptManager conceptMgr;

            long n = 0;
            double mean = 0d, M2 = 0d;

            private STDCalculator(ConceptManager conceptMgr) {
                this.conceptMgr = conceptMgr;
            }

            void accumulate(double value) {
                n += 1;
                double delta = value - mean;
                mean += delta / (double) n;
                double delta2 = value - mean;
                M2 += delta * delta2;
            }

            Optional<Value<?>> std() {
                if (n < 2) return Optional.empty();
                else return Optional.of(createValue(conceptMgr, sqrt(M2 / (double) (n - 1))));
            }
        }

        private static class NumericComparator implements Comparator<Value<?>> {

            static NumericComparator natural = new NumericComparator();

            public static Comparator<Value<?>> natural() {
                return natural;
            }

            @Override
            public int compare(Value<?> a, Value<?> b) {
                return compareValue(a, b);
            }

            private <T, U> int compareValue(Value<T> a, Value<U> b) {
                assert (a.isLong() || a.isDouble()) && (b.isLong() || b.isDouble());
                if (a.isLong() && b.isLong()) return a.asLong().value().compareTo(b.asLong().value());
                else {
                    Double aValue = (double) a.value();
                    return aValue.compareTo((double) b.value());
                }
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

        private final Getter getter;
        private final TypeQLGet.Group query;
        private final Context.Query context;

        public Group(Getter getter, TypeQLGet.Group query, Context.Query context) {
            this.getter = getter;
            this.query = query;
            this.context = context;
            this.context.producer(Either.first(EXHAUSTIVE));
        }

        public FunctionalIterator<ConceptMapGroup> execute() {
            // TODO: Replace this temporary implementation of TypeQL Match Group query with a native grouping traversal
            List<ConceptMapGroup> answerGroups = new ArrayList<>();
            getter.execute(context).stream().collect(groupingBy(a -> a.get(query.var())))
                    .forEach((o, cm) -> answerGroups.add(new ConceptMapGroup(o, cm)));
            return iterate(answerGroups);
        }

        public static class Aggregator {

            private final ConceptManager conceptMgr;
            private final Group group;
            private final TypeQLGet.Group.Aggregate query;

            public Aggregator(ConceptManager conceptMgr, Group group, TypeQLGet.Group.Aggregate query) {
                this.conceptMgr = conceptMgr;
                this.group = group;
                this.query = query;
            }

            public FunctionalIterator<ValueGroup> execute() {
                // TODO: Replace this temporary implementation of TypeQL Match Group query with a native grouping traversal
                List<ValueGroup> valueGroups = new ArrayList<>();
                group.getter.execute(group.context).stream()
                        .collect(groupingBy(a -> a.get(query.group().var()), aggregator(conceptMgr, query.method(), query.var())))
                        .forEach((o, n) -> valueGroups.add(new ValueGroup(o, n)));
                return iterate(valueGroups);
            }
        }
    }
}
