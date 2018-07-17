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
import ai.grakn.generator.GraknTxs.Open;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.ConceptMap;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.VarPropertyInternal;
import com.google.common.collect.Sets;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class PatternPropertyTests {

    @Property
    public void patternsWithDifferentVariablesAndTheSameProperties_ShouldNotBeEquivalent(Var var1, Var var2, Set<VarProperty> properties){
        assumeThat(var1, not(is(var2)));
        VarPatternAdmin varPattern1 = Patterns.varPattern(var1, properties);
        VarPatternAdmin varPattern2 = Patterns.varPattern(var2, properties);
        assertNotEquals(varPattern1, varPattern2);
    }

    @Property
    public void theConjunctionOfTwoPatterns_ShouldBeContainedInTheResultingPattern(Pattern pattern1, Pattern pattern2){
        Set<VarPattern> union = Sets.union(pattern1.admin().varPatterns(), pattern2.admin().varPatterns());

        Pattern conjunction = pattern1.and(pattern2);
        assertEquals(union, conjunction.admin().varPatterns());
    }

    @Property
    public void theDisjunctionOfTwoPatterns_ShouldBeContainedInTheResultingPattern(Pattern pattern1, Pattern pattern2){
        Set<VarPattern> union = Sets.union(pattern1.admin().varPatterns(), pattern2.admin().varPatterns());

        Pattern disjunction = pattern1.or(pattern2);
        assertEquals(union, disjunction.admin().varPatterns());
    }

    @Property
    public void ifAPropertyUniquelyIdentifiesAConcept_0or1ResultsAreReturned(@Open GraknTx tx, VarProperty property){
        if(VarPropertyInternal.from(property).uniquelyIdentifiesConcept()){
            List<ConceptMap> results = tx.graql().match(Patterns.varPattern(Graql.var("x"), Collections.singleton(property))).get().execute();
            assertThat(results, hasSize(lessThanOrEqualTo(1)));
        }
    }
}
