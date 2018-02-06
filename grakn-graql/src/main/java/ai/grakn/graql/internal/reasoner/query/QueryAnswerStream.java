/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.NeqPredicate;
import ai.grakn.graql.internal.reasoner.iterator.LazyAnswerIterator;
import ai.grakn.graql.internal.reasoner.utils.Pair;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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

    private static Set<Answer> findMatchingAnswers(Var var, Concept con, Map<Pair<Var, Concept>, Set<Answer>> inverseMap){
        Pair<Var, Concept> key = new Pair<>(var, con);
        return inverseMap.containsKey(key)? inverseMap.get(key) : new HashSet<>();
    }

    private static Set<Answer> findMatchingAnswers(Answer answer, Map<Pair<Var, Concept>, Set<Answer>> inverseMap, Var joinVar){
        Pair<Var, Concept> key = new Pair<>(joinVar, answer.get(joinVar));
        return inverseMap.containsKey(key)? inverseMap.get(key) : new HashSet<>();
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


