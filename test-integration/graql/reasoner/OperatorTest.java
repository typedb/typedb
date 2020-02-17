/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import grakn.core.graql.reasoner.operator.Operator;
import grakn.core.graql.reasoner.operator.Operators;
import grakn.core.graql.reasoner.operator.TypeContext;
import graql.lang.pattern.Pattern;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graql.lang.Graql.and;
import static graql.lang.Graql.var;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class OperatorTest {

    private static TypeContext ctx = new MockTypeContext();

    @Test
    public void computeIdentity(){
        Pattern input = and(var().isa("thing"));
        Assert.assertEquals(input, Operators.identity().apply(input,null).iterator().next());
    }

    @Test
    public void whenGeneraliseTypeOfASingleStatement_weDecrementTheLabel(){
        Pattern input = and(var("x").isa("subEntity"));
        Pattern expectedOutput = and(var("x").isa("baseEntity"));

        Pattern output = Iterables.getOnlyElement(
                Operators.typeGeneralise().apply(input, ctx).collect(Collectors.toSet())
                );
        assertEquals(expectedOutput, output);

        Pattern secondOutput = Iterables.getOnlyElement(
                Operators.typeGeneralise().apply(output, ctx).collect(Collectors.toSet())
        );
        assertEquals(
                and(var("x").isa("entity")),
                secondOutput);

        Pattern thirdOutput = Iterables.getOnlyElement(
                Operators.typeGeneralise().apply(secondOutput, ctx).collect(Collectors.toSet())
        );
        assertEquals(
                and(var("x").isa(var("xtype"))),
                thirdOutput
        );
    }

    @Test
    public void whenApplyingGeneraliseTypeOperator_weDecrementLabelsOneByOne(){
        Pattern input = and(
                var("r").rel(var("x")).rel(var("y")).isa("subRelation"),
                var("x").isa("subEntity"),
                var("y").isa("subEntity")
                );
        Set<Pattern> expectedOutput = Sets.newHashSet(
                and(
                        var("r").rel(var("x")).rel(var("y")).isa("subRelation"),
                        var("x").isa("baseEntity"),
                        var("y").isa("subEntity")),
                and(
                        var("r").rel(var("x")).rel(var("y")).isa("subRelation"),
                        var("x").isa("subEntity"),
                        var("y").isa("baseEntity")),
                and(
                        var("r").rel(var("x")).rel(var("y")).isa("baseRelation"),
                        var("x").isa("subEntity"),
                        var("y").isa("subEntity"))
        );

        Set<Pattern> output = Operators.typeGeneralise().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(expectedOutput, output);
    }

    @Test
    public void whenApplyingTypeGenOperatorMultipleTimes_patternsConvergeToEmpty(){
        Pattern input = and(
                var("r").rel(var("x")).rel(var("y")).isa("subRelation"),
                var("x").isa("subEntity")
        );
        testOperatorConvergence(input, Lists.newArrayList(Operators.typeGeneralise()));
    }

    @Test
    public void whenGeneralisingRoleOfASingleRPStatement_weDecrementTheLabel(){
        Pattern input = and(var("r").rel("baseRole", var("x")));
        Pattern expectedOutput = and(var("r").rel("role", var("x")));

        Pattern output = Iterables.getOnlyElement(
                Operators.roleGeneralise().apply(input, ctx).collect(Collectors.toSet())
        );
        assertEquals(expectedOutput, output);
    }

    @Test
    public void whenApplyingGeneraliseRoleOperator_weDecrementLabelsOneByOne(){
        Pattern input = and(
                var("r")
                        .rel("baseRole", var("x"))
                        .rel("subRole", var("y"))
                        .rel("role", var("z")),
                var("x").isa("subEntity"),
                var("y").isa("subEntity")
        );
        Set<Pattern> expectedOutput = Sets.newHashSet(
                and(
                        var("r")
                                .rel("baseRole", var("x"))
                                .rel("subRole", var("y"))
                                .rel(var("zrole"), var("z")),
                        var("x").isa("subEntity"),
                        var("y").isa("subEntity")),
                and(
                        var("r")
                                .rel("baseRole", var("x"))
                                .rel("baseRole", var("y"))
                                .rel("role", var("z")),
                        var("x").isa("subEntity"),
                        var("y").isa("subEntity")),
                and(
                        var("r")
                                .rel("role", var("x"))
                                .rel("subRole", var("y"))
                                .rel("role", var("z")),
                        var("x").isa("subEntity"),
                        var("y").isa("subEntity"))
        );

        Set<Pattern> output = Operators.roleGeneralise().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(expectedOutput, output);
    }

    @Test
    public void whenApplyingRoleGenOperatorMultipleTimes_patternsConvergeToEmpty(){
        Pattern input = and(
                var("r")
                        .rel("baseRole", var("x"))
                        .rel("subRole", var("y"))
                        .rel("role", var("z")),
                var("x").isa("subEntity"),
                var("y").isa("subEntity")
        );
        testOperatorConvergence(input, Lists.newArrayList(Operators.roleGeneralise()));
    }

    @Test
    public void whenApplyingRemoveSubOperatorOnASingleSubPattern_patternWithNoSubsIsReturned(){
        Pattern singleStatementInput = and(var("x").isa("subEntity").id("V123"));

        Pattern multiStatementInput = and(
                var("x").isa("subEntity"),
                var("x").id("V123")
        );
        Set<Pattern> expectedOutput = Sets.newHashSet(and(var("x").isa("subEntity")));

        Set<Pattern> output = Operators.removeSubstitution().apply(singleStatementInput, ctx).collect(Collectors.toSet());
        assertEquals(expectedOutput, output);

        output = Operators.removeSubstitution().apply(multiStatementInput, ctx).collect(Collectors.toSet());
        assertEquals(expectedOutput, output);
    }

    @Test
    public void whenRemovingSubFromPatternWithNoSub_weDoNoop(){
        Pattern input = and(var("x").isa("subEntity"));
        Set<Pattern> output = Operators.removeSubstitution().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(input), output);
    }

    @Test
    public void whenApplyingRemoveSubOperator_weGenerateCartesianProductOfPossibleSubConfigurations(){
        Pattern input = and(
                var("r").rel(var("x")).rel(var("y")),
                var("x").id("V123"),
                var("y").id("V456"),
                var("z").id("V789")
        );

        Set<Pattern> expectedOutput = Sets.newHashSet(
                and(
                        var("r").rel(var("x")).rel(var("y")),
                        var("x").id("V123")),
                and(
                        var("r").rel(var("x")).rel(var("y")),
                        var("y").id("V456")),
                and(
                        var("r").rel(var("x")).rel(var("y")),
                        var("z").id("V789")),
                and(
                        var("r").rel(var("x")).rel(var("y")),
                        var("x").id("V123"),
                        var("y").id("V456")),
                and(
                        var("r").rel(var("x")).rel(var("y")),
                        var("x").id("V123"),
                        var("z").id("V789")),
                and(
                        var("r").rel(var("x")).rel(var("y")),
                        var("y").id("V456"),
                        var("z").id("V789")),
                and(
                        var("r").rel(var("x")).rel(var("y")))
        );

        Set<Pattern> output = Operators.removeSubstitution().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(expectedOutput, output);
    }

    @Test
    public void whenApplyingRemoveSubOperatorToPatternWithNoSubs_weDoNoop(){
        Pattern input = and(
                var("r").rel(var("x")).rel(var("y")),
                var("x").isa("subEntity"),
                var("y").isa("subEntity")
        );
        Set<Pattern> output = Operators.removeSubstitution().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(input), output);
    }

    @Test
    public void whenApplyingRemoveRoleplayerOperator_weGenerateCartesianProductOfPossibleRPConfigurations(){
        Pattern input = and(
                var("r").rel(var("x")).rel(var("y")).rel(var("z")).isa("baseRelation")
        );

        Set<Pattern> expectedOutput = Sets.newHashSet(
                and(var("r").isa("baseRelation")),
                and(var("r").rel(var("x")).rel(var("y")).isa("baseRelation")),
                and(var("r").rel(var("x")).rel(var("z")).isa("baseRelation")),
                and(var("r").rel(var("y")).rel(var("z")).isa("baseRelation")),
                and(var("r").rel(var("x")).isa("baseRelation")),
                and(var("r").rel(var("y")).isa("baseRelation")),
                and(var("r").rel(var("z")).isa("baseRelation"))
        );

        Set<Pattern> output = Operators.removeRoleplayer().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(expectedOutput, output);
    }

    @Test
    public void whenApplyingRemoveRoleplayerOperatorToPatternWithNoRPs_weDoNoop(){
        Pattern input = and(var("x").isa("subEntity"));
        Set<Pattern> output = Operators.removeRoleplayer().apply(input, ctx).collect(Collectors.toSet());
        assertEquals(Sets.newHashSet(input), output);
    }

    @Test
    public void whenApplyingDifferentOperatorsConsecutively_weConvergeToEmptyPattern(){
        Pattern input = and(
                var("r")
                        .rel("subRole", var("x"))
                        .rel("subRole", var("y"))
                        .rel("subRole", var("z")),
                var("x").isa("subEntity"),
                var("x").id("V123"),
                var("y").isa("subEntity"),
                var("y").id("V456"),
                var("z").isa("subEntity"),
                var("z").id("V789")
        );


        testOperatorConvergence(input, Lists.newArrayList(
                Operators.removeSubstitution(),
                Operators.typeGeneralise(),
                Operators.roleGeneralise(),
                Operators.typeGeneralise(),
                Operators.roleGeneralise())
        );
    }
    
    private void testOperatorConvergence(Pattern input, List<Operator> ops) {
        Set<Pattern> output = Sets.newHashSet(input);
        while (!output.isEmpty()){
            Stream<Pattern> pstream = output.stream();
            for(Operator op : ops){
                pstream = pstream.flatMap(p -> op.apply(p, ctx));
            }
            output = pstream.collect(Collectors.toSet());
        }
        assertTrue(output.isEmpty());
    }
}
