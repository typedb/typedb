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

package grakn.core.graql.executor;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import graql.lang.Graql;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import static java.lang.Math.sqrt;

public class AggregateExecutor {

    private static Number getValue(ConceptMap answer, Variable var) {
        Object value = answer.get(var).asAttribute().value();

        if (value instanceof Number) return (Number) value;
        else throw new RuntimeException("Invalid attempt to compare non-Numbers in Max Aggregate function");
    }

    public static List<Numeric> aggregate(Stream<ConceptMap> answers, Graql.Token.Aggregate.Method method, Variable var) {
        switch (method) {
            case COUNT:
                return count(answers);
            case MAX:
                return max(answers, var);
            case MEAN:
                return mean(answers, var);
            case MEDIAN:
                return median(answers, var);
            case MIN:
                return min(answers, var);
            case SUM:
                return sum(answers, var);
            default:
                throw new IllegalArgumentException("Invalid Aggregate method");
        }
    }

    static List<Numeric> count(Stream<ConceptMap> answers) {
        return Collections.singletonList(new Numeric(answers.count()));
    }

    static List<Numeric> max(Stream<ConceptMap> answers, Variable var) {
        PrimitiveNumberComparator comparator = new PrimitiveNumberComparator();
        Number number = answers.map(answer -> getValue(answer, var)).max(comparator).orElse(null);

        if (number == null) return Collections.emptyList();
        else return Collections.singletonList( new Numeric(number));
    }

    static List<Numeric> mean(Stream<ConceptMap> answers, Variable var) {
        double mean = answers
                .mapToDouble(answer -> getValue(answer, var).doubleValue())
                .average()
                .orElse(Double.NaN);

        if (Double.isNaN(mean)) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(new Numeric(mean));
        }
    }

    static List<Numeric> median(Stream<ConceptMap> answers, Variable var) {
        MedianFinder medianFinder = new MedianFinder();
        answers.map(a -> getValue(a, var)).forEach(medianFinder::addNum);

        Number median = medianFinder.findMedian();

        if (median == null) return Collections.emptyList();
        else return Collections.singletonList(new Numeric(median));
    }

    static List<Numeric> min(Stream<ConceptMap> answers, Variable var) {
        PrimitiveNumberComparator comparator = new PrimitiveNumberComparator();
        Number number = answers
                .map(answer -> getValue(answer, var))
                .min(comparator)
                .orElse(null);

        if (number == null) return Collections.emptyList();
        else return Collections.singletonList(new Numeric(number));
    }

    static List<Numeric> std(Stream<ConceptMap> answers, Variable var) {
        Stream<Double> numStream = answers.map(result -> result.get(var)
                .<Number>asAttribute().value()
                .doubleValue());

        Iterable<Double> data = numStream::iterator;

        // Online algorithm to calculate unbiased sample standard deviation
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
        long n = 0;
        double mean = 0d;
        double M2 = 0d;

        for (double x : data) {
            n += 1;
            double delta = x - mean;
            mean += delta / (double) n;
            double delta2 = x - mean;
            M2 += delta*delta2;
        }

        if (n < 2) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(new Numeric(sqrt(M2 / (double) (n - 1))));
        }
    }

    static List<Numeric> sum(Stream<ConceptMap> answers, Variable var) {
        // initial value is set to null so that we can return null if there is no Answers to consume
        Number number = answers
                .map(answer -> getValue(answer, var))
                .reduce(null, AggregateExecutor::addNumbers);

        if (number == null) return Collections.emptyList();
        else return Collections.singletonList(new Numeric(number));
    }

    private static Number addNumbers(Number x, Number y) {
        // if this method is called, then there is at least one number to apply SumAggregate to, thus we set x back to 0
        if (x == null) x = 0;

        // This method is necessary because Number doesn't support '+' because java!
        if (x instanceof Long && y instanceof Long) {
            return x.longValue() + y.longValue();
        } else if (x instanceof Long) {
            return x.longValue() + y.doubleValue();
        } else if (y instanceof Long) {
            return x.doubleValue() + y.longValue();
        } else {
            return x.doubleValue() + y.doubleValue();
        }
    }

    private static class MedianFinder {
        PriorityQueue<Number> maxHeap; //lower half
        PriorityQueue<Number> minHeap; //higher half

        MedianFinder() {
            maxHeap = new PriorityQueue<>(Collections.reverseOrder());
            minHeap = new PriorityQueue<>();
        }

        // Adds a number into the data structure.
        void addNum(Number num) {
            maxHeap.offer(num);
            minHeap.offer(maxHeap.poll());

            if (maxHeap.size() < minHeap.size()) {
                maxHeap.offer(minHeap.poll());
            }
        }

        // Returns the median of current data stream
        Number findMedian() {
            if (maxHeap.isEmpty() && minHeap.isEmpty()) {
                return null;

            } else if (maxHeap.size() == minHeap.size()) {
                return addNumbers(maxHeap.peek(), minHeap.peek()).doubleValue() / 2;

            } else {
                return maxHeap.peek();
            }
        }
    }

    /**
     * A Comparator class to compare the 2 numbers only if they have the same primitive type.
     */
    public static class PrimitiveNumberComparator implements Comparator<Number> {

        @Override
        public int compare(Number a, Number b) {
            if (((Object) a).getClass().equals(((Object) b).getClass()) && a instanceof Comparable) {
                return ((Comparable) a).compareTo(b);
            }

            throw new RuntimeException("Invalid attempt to compare non-comparable primitive type of Numbers in Aggregate function");
        }
    }
}
