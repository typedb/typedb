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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.property.graql;

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static ai.grakn.graql.internal.pattern.Patterns.varPattern;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(JUnitQuickcheck.class)
public class GetQueryPropertyTest {

    // If a generated query has more than this many answers, it is skipped.
    // This is a measure to increase test performance by avoiding queries with huge numbers of answers
    private static final int MAX_ANSWERS = 100;

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void matchIsAMorphism_From_PatternsWithConjunction_To_AnswersWithNaturalJoin(
            @Open GraknTx tx, Pattern a, Pattern b
    ) {
        System.out.println(a);
        System.out.println(b);
        Set<ConceptMap> matchA = matchSet(tx, a);
        Set<ConceptMap> matchB = matchSet(tx, b);
        String formatMsg = "\nmatch(a Λ b) != match(a) ⋈ match(b)\na = %s\nb = %s \nmatch(a) = %s\nmatch(b) = %s\n";
        String message = String.format(formatMsg, a, b, matchA, matchB);

        assertEquals(message,
                naturalJoin(matchA, matchB),
                matchSet(tx, (a).and(b))
        );
    }

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void conjunctionIsCommutative(@Open GraknTx tx, Pattern a, Pattern b) {
        assertEquivalent(tx,
                (a).and(b),
                (b).and(a)
        );
    }

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void conjunctionIsAssociative(@Open GraknTx tx, Pattern a, Pattern b, Pattern c) {
        assertEquivalent(tx,
                (a).and( (b).and(c) ),
                ( (a).and(b) ).and(c)
        );
    }

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void conjunctionIsDistributiveOverDisjunction(@Open GraknTx tx, Pattern a, Pattern b, Pattern c) {
        assertEquivalent(tx,
                (a).and( (b).or(c) ),
                ( (a).and(b) ).or( (a).and(c) )
        );
    }

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void disjunctionIsCommutative(@Open GraknTx tx, Pattern a, Pattern b) {
        assertEquivalent(tx,
                (a).or(b),
                (b).or(a)
        );
    }

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void disjunctionIsAssociative(@Open GraknTx tx, Pattern a, Pattern b, Pattern c) {
        assertEquivalent(tx,
                (a).or( (b).or(c) ),
                ( (a).or(b) ).or(c)
        );
    }

    @Property
    public void varPatternIsAMorphism_From_VarPropertiesWithUnion_To_PatternsWithConjunction(
            @Open GraknTx tx,
            Var var, @Size(max=10) Set<VarProperty> a, @Size(max=10) Set<VarProperty> b
    ) {
        assertEquivalent(tx,
                varPattern(var, Sets.union(a, b)),
                (varPattern(var, a)).and(varPattern(var, b))
        );
    }

    @Ignore("Currently failing randomly. This needs to be fixed or removed")
    @Property
    public void anyPropertyCanBeExecutedOnAMatchQuery(@Open GraknTx tx, VarProperty property){
        VarPatternAdmin pattern = Patterns.varPattern(Graql.var(), Collections.singleton(property));
        try {
            tx.graql().match(pattern).get().execute();
        } catch(GraqlQueryException e){
            //Ignore
        }
    }

    @Property
    public void ifAPatternCanBeUsedInAnInsertQuery_ItCanBeUsedInGetQuery(@Open GraknTx tx, VarPattern pattern){
        executeAsGetQuery(tx, pattern, (p) -> tx.graql().insert(p));
    }

    @Property
    public void ifAPatternCanBeUsedInADefineQuery_ItCanBeUsedInGetQuery(@Open GraknTx tx, VarPattern pattern){
        executeAsGetQuery(tx, pattern, (p) -> tx.graql().define(p));
    }

    private void executeAsGetQuery(@Open GraknTx tx, VarPattern pattern, Function<VarPattern, Query> queryBuilder){
        try {
            queryBuilder.apply(pattern).execute();
        } catch (GraknTxOperationException | GraqlQueryException e){
            return ;
        }

        assertFalse(tx.graql().match(pattern).get().execute().isEmpty());
    }

    private Set<ConceptMap> matchSet(GraknTx tx, Pattern pattern) {
        final int[] count = {0};

        try {
            return tx.graql().match(pattern).stream().peek(answer -> {
                count[0] += 1;
                // This assumption will cause the query execution to fail fast if there are too many results
                assumeTrue(count[0] < MAX_ANSWERS);
            }).collect(toSet());
        } catch (GraqlQueryException e) {
            // If there is a problem with the query, we assume it has zero results
            // TODO: Maybe this assumption isn't true. Also maybe the query execution shouldn't throw
            return ImmutableSet.of();
        }
    }

    /**
     * Perform a natural join on two sets of results.
     * This involves combining answers when all their common variables are bound to the same concept. When both sets
     * of answers have completely disjoint variables, this is equivalent to the cartesian product.
     */
    private Set<ConceptMap> naturalJoin(Set<ConceptMap> answersA, Set<ConceptMap> answersB) {
        return answersA.stream()
                .flatMap(a -> answersB.stream().map(b -> joinAnswer(a, b)).flatMap(CommonUtil::optionalToStream))
                .collect(toSet());
    }

    private Optional<ConceptMap> joinAnswer(ConceptMap answerA, ConceptMap answerB) {
        Map<Var, Concept> answer = Maps.newHashMap(answerA.map());
        answer.putAll(answerB.map());

        for (Var var : Sets.intersection(answerA.vars(), answerB.vars())) {
            if (!answerA.get(var).equals(answerB.get(var))) {
                return Optional.empty();
            }
        }

        return Optional.of(new ConceptMapImpl(answer));
    }

    private void assertEquivalent(GraknTx tx, Pattern patternA, Pattern patternB) {
        assertEquals(
                matchSet(tx, patternA),
                matchSet(tx, patternB)
        );
    }
}