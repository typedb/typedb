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
 */

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;

import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper class for an answer stream providing higher level filtering facilities
 * as well as unification and join operations.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswerStream {

    public static Map<VarName, Concept> varFilterOperator(Map<VarName, Concept> answer, Set<VarName> vars) {
        Map<VarName, Concept> filteredAnswer = new HashMap<>();
        vars.stream()
                .filter(answer::containsKey)
                .forEach(var -> filteredAnswer.put(var, answer.get(var)));
        return filteredAnswer;
    }

    public static boolean knownFilter(Map<VarName, Concept> answer, Stream<Map<VarName, Concept>> known) {
        boolean isKnown = false;
        Iterator<Map<VarName, Concept>> it = known.iterator();
        while (it.hasNext() && !isKnown) {
            Map<VarName, Concept> knownAnswer = it.next();
            isKnown = knownAnswer.entrySet().containsAll(answer.entrySet());
        }
        return !isKnown;
    }

    public static boolean nonEqualsFilter(Map<VarName, Concept> answer, Set<NotEquals> atoms) {
        if(atoms.isEmpty()) return true;
        boolean pass = true;
        Iterator<NotEquals> it = atoms.iterator();
        while (it.hasNext() && pass) {
            pass = NotEquals.notEqualsOperator(answer, it.next());
        }
        return pass;
    }

    public static boolean subFilter(Map<VarName, Concept> answer, Set<IdPredicate> subs){
        if (subs.isEmpty()) return true;
        boolean pass = true;
        Iterator<IdPredicate> it = subs.iterator();
        while (it.hasNext() && pass) {
            IdPredicate sub = it.next();
            pass = answer.get(sub.getVarName()).getId().equals(sub.getPredicate());
        }
        return pass;
    }

    public static boolean entityTypeFilter(Map<VarName, Concept> answer, Set<TypeAtom> types){
        if (types.isEmpty()) return true;
        boolean pass = true;
        Iterator<TypeAtom> it = types.stream().iterator();
        while( it.hasNext() && pass){
            TypeAtom type = it.next();
            VarName var = type.getVarName();
            Type t = type.getType();
            pass = answer.get(var).asInstance().type().equals(t);
        }
        return pass;
    }

    public static Stream<Map<VarName, Concept>> permuteOperator(Map<VarName, Concept> answer, Set<Map<VarName, VarName>> unifierSet){
        if (unifierSet.isEmpty()) return Stream.of(answer);
        return unifierSet.stream().flatMap(unifiers -> Stream.of(QueryAnswers.unify(answer, unifiers)));
    }

    public static Map<VarName, Concept> joinOperator(Map<VarName, Concept> m1, Map<VarName, Concept> m2){
        boolean isCompatible = true;
        Set<VarName> joinVars = Sets.intersection(m1.keySet(), m2.keySet());
        Iterator<VarName> it = joinVars.iterator();
        while(it.hasNext() && isCompatible) {
            VarName var = it.next();
            isCompatible = m1.get(var).equals(m2.get(var));
        }
        if (isCompatible) {
            Map<VarName, Concept> merged = new HashMap<>(m1);
            merged.putAll(m2);
            return merged;
        } else return new HashMap<>();
    }

    public static final BiFunction<Map<VarName, Concept>, Set<VarName>, Stream<Map<VarName, Concept>>> varFilterFunction = (a, vars) -> {
        Map<VarName, Concept> filteredAnswer = varFilterOperator(a, vars);
        return filteredAnswer.isEmpty() ? Stream.empty() : Stream.of(filteredAnswer);
    };

    public static final BiFunction<Map<VarName, Concept>, Set<Map<VarName, VarName>>, Stream<Map<VarName, Concept>>> permuteFunction = QueryAnswerStream::permuteOperator;

    public static final BiFunction<Map<VarName, Concept>, Map<VarName, Concept>, Stream<Map<VarName, Concept>>> joinFunction = (a1, a2) -> {
        Map<VarName, Concept> merged = joinOperator(a1, a2);
        return merged.isEmpty()? Stream.empty(): Stream.of(merged);
    };

    /**
     * unify answer stream by applying unifiers
     * @param answers stream of answers to be unified
     * @param unifiers to apply on stream elements
     * @return unified answer stream
     */
    public static Stream<Map<VarName, Concept>> unify(Stream<Map<VarName, Concept>> answers, Map<VarName, VarName> unifiers) {
        return answers.map(ans -> QueryAnswers.unify(ans, unifiers));
    }

    /**
     * lazy stream join
     * @param stream left stream operand
     * @param stream2 right stream operand
     * @return joined stream
     */
    public static Stream<Map<VarName, Concept>> join(Stream<Map<VarName, Concept>> stream, Stream<Map<VarName, Concept>> stream2) {
        return join(joinFunction, stream, stream2);
    }

    /**
     * lazy stream join with quasi sideways information propagation
     * @param stream left stream operand
     * @param stream2 right stream operand
     * @return joined stream
     */
    public static Stream<Map<VarName, Concept>> join(Stream<Map<VarName, Concept>> stream, Stream<Map<VarName, Concept>> stream2, Set<VarName> joinVars) {
        LazyIterator<Map<VarName, Concept>> l2 = new LazyIterator<>(stream2);
        return stream.flatMap(a1 -> {
            Stream<Map<VarName, Concept>> answerStream = l2.stream();
            for (VarName v : joinVars) answerStream = answerStream.filter(ans -> ans.get(v).equals(a1.get(v)));
            return answerStream.flatMap(a2 -> {
                Map<VarName, Concept> merged = new HashMap<>(a1);
                merged.putAll(a2);
                return Stream.of(merged);
            });
        });
    }

    /**
     * perform a lazy join operation on two streams (stream and stream2)
     * @param function joining function
     * @param s1 left operand of join operation
     * @param s2 right operand of join operation
     * @return joined stream
     */
    public static <T> Stream<T> join(BiFunction<T, T, Stream<T>> function, Stream<T> s1, Stream<T> s2) {
        LazyIterator<T> l2 = new LazyIterator<>(s2);
        return s1.flatMap(a1 -> l2.stream().flatMap(a2 -> function.apply(a1,a2)));
    }
}


