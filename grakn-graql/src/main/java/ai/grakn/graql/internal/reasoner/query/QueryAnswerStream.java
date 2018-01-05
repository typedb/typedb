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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import ai.grakn.graql.internal.reasoner.utils.Pair;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Wrapper class providing higher level stream operations streams of {@link Answer}.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class QueryAnswerStream {

    public static boolean knownFilter(Answer answer, Stream<Answer> known) {
        Iterator<Answer> it = known.iterator();
        while (it.hasNext()) {
            Answer knownAnswer = it.next();
            if(knownAnswer.entrySet().containsAll(answer.entrySet())){
                return false;
            }
        }
        return true;
    }

    static boolean knownFilterWithInverse(Answer answer, Map<Pair<Var, Concept>, Set<Answer>> stream2InverseMap) {
        Iterator<Map.Entry<Var, Concept>> eit = answer.entrySet().iterator();
        Map.Entry<Var, Concept> entry = eit.next();
        Set<Answer> matchAnswers = findMatchingAnswers(entry.getKey(), entry.getValue(), stream2InverseMap);
        while(eit.hasNext()){
            entry = eit.next();
            matchAnswers = Sets.intersection(matchAnswers, findMatchingAnswers(entry.getKey(), entry.getValue(), stream2InverseMap));
        }
        for (Answer knownAnswer : matchAnswers) {
            if (knownAnswer.entrySet().containsAll(answer.entrySet())) {
                return false;
            }
        }
        return true;
    }

    static boolean nonEqualsFilter(Answer answer, Set<NeqPredicate> atoms) {
        if(atoms.isEmpty()) return true;
        for (NeqPredicate atom : atoms) {
            if (!NeqPredicate.notEqualsOperator(answer, atom)) {
                return false;
            }
        }
        return true;
    }

    public static boolean subFilter(Answer answer, Set<IdPredicate> subs){
        if (subs.isEmpty()) return true;
        for (IdPredicate sub : subs) {
            if (!answer.get(sub.getVarName()).getId().equals(sub.getPredicate())) {
                return false;
            }
        }
        return true;
    }

    static boolean entityTypeFilter(Answer answer, Set<TypeAtom> types){
        if (types.isEmpty()) return true;
        for (TypeAtom type : types){
            Var var = type.getVarName();
            SchemaConcept t = type.getSchemaConcept();
            if (t.subs().noneMatch(sub -> sub.equals(answer.get(var).asThing().type()))) {
                return false;
            }
        }
        return true;
    }

    private static Answer joinOperator(Answer m1, Answer m2){
        boolean isCompatible = true;
        Set<Var> joinVars = Sets.intersection(m1.vars(), m2.vars());
        Iterator<Var> it = joinVars.iterator();
        while(it.hasNext() && isCompatible) {
            Var var = it.next();
            isCompatible = m1.get(var).equals(m2.get(var));
        }
        return isCompatible? m1.merge(m2) : new QueryAnswer();
    }

    private static final BiFunction<Answer, Answer, Stream<Answer>> joinFunction = (a1, a2) -> {
        Answer merged = joinOperator(a1, a2);
        return merged.isEmpty()? Stream.empty(): Stream.of(merged);
    };

    /**
     * perform a lazy join operation on two streams (stream and stream2)
     * @param function joining function
     * @param s1 left operand of join operation
     * @param s2 right operand of join operation
     * @return joined stream
     */
    private static <T> Stream<T> join(BiFunction<T, T, Stream<T>> function, Stream<T> s1, Stream<T> s2) {
        LazyIterator<T> l2 = new LazyIterator<>(s2);
        return s1.flatMap(a1 -> l2.stream().flatMap(a2 -> function.apply(a1,a2)));
    }

    private static Set<Answer> findMatchingAnswers(Var var, Concept con, Map<Pair<Var, Concept>, Set<Answer>> inverseMap){
        Pair<Var, Concept> key = new Pair<>(var, con);
        return inverseMap.containsKey(key)? inverseMap.get(key) : new HashSet<>();
    }

    private static Set<Answer> findMatchingAnswers(Answer answer, Map<Pair<Var, Concept>, Set<Answer>> inverseMap, Var joinVar){
        Pair<Var, Concept> key = new Pair<>(joinVar, answer.get(joinVar));
        return inverseMap.containsKey(key)? inverseMap.get(key) : new HashSet<>();
    }

    /**
     * lazy stream join
     * @param stream left stream operand
     * @param stream2 right stream operand
     * @return joined stream
     */
    public static Stream<Answer> join(Stream<Answer> stream, Stream<Answer> stream2) {
        return join(joinFunction, stream, stream2);
    }

    /**
     * lazy stream join with quasi- sideways information propagation
     * @param stream left stream operand
     * @param stream2 right stream operand
     * @param joinVars intersection on variables of two streams
     * @return joined stream
     */
    public static Stream<Answer> join(Stream<Answer> stream, Stream<Answer> stream2, ImmutableSet<Var> joinVars) {
        LazyAnswerIterator l2 = new LazyAnswerIterator(stream2);
        return stream.flatMap(a1 -> {
            Stream<Answer> answerStream = l2.stream();
            answerStream = answerStream.filter(ans -> {
                for(Var v: joinVars) {
                    if (!ans.get(v).equals(a1.get(v))) {
                        return false;
                    }
                }
                return true;
            });
            return answerStream.map(a -> a.merge(a1));
        });
    }

    /**
     * lazy stream join with fast lookup from inverse answer map
     * @param stream left stream operand
     * @param stream2 right stream operand
     * @param stream2InverseMap inverse map of right operand from cache
     * @param joinVars intersection on variables of two streams
     * @return joined stream
     */
    static Stream<Answer> joinWithInverse(Stream<Answer> stream,
                                          Stream<Answer> stream2,
                                          Map<Pair<Var, Concept>, Set<Answer>> stream2InverseMap,
                                          ImmutableSet<Var> joinVars) {
        if (joinVars.isEmpty()){
            LazyAnswerIterator l2 = new LazyAnswerIterator(stream2);
            return stream.flatMap(a1 -> l2.stream().map(a -> a.merge(a1)));
        }
        return stream.flatMap(a1 -> {
            Iterator<Var> vit = joinVars.iterator();
            Set<Answer> matchAnswers = findMatchingAnswers(a1, stream2InverseMap, vit.next());
            while(vit.hasNext()){
                matchAnswers = Sets.intersection(matchAnswers, findMatchingAnswers(a1, stream2InverseMap, vit.next()));
            }
            return matchAnswers.stream().map(a -> a.merge(a1));
        });
    }
}


