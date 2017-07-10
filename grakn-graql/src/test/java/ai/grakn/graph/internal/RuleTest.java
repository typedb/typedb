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

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static ai.grakn.util.ErrorMessage.NULL_VALUE;
import static ai.grakn.util.Schema.VertexProperty.RULE_LHS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

//NOTE: This test is inside the graql module due to the inability to have graql constructs inside the graph module
public class RuleTest {
    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private Pattern lhs;
    private Pattern rhs;
    private GraknGraph graknGraph;
    private GraknSession session;

    @Before
    public void setupRules(){
        session = Grakn.session(Grakn.IN_MEMORY, "absd");
        graknGraph = Grakn.session(Grakn.IN_MEMORY, "absd").open(GraknTxType.WRITE);
        lhs = graknGraph.graql().parsePattern("$x isa entity-type");
        rhs = graknGraph.graql().parsePattern("$x isa entity-type");
    }

    @After
    public void closeSession() throws Exception {
        graknGraph.close();
        session.close();
    }

    @Test
    public void whenCreatingRulesWithNullValues_Throw() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.putRule(lhs, rhs);
        assertEquals(lhs, rule.getLHS());
        assertEquals(rhs, rule.getRHS());

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(NULL_VALUE.getMessage(RULE_LHS));

        conceptType.putRule(null, null);
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidGraphException {
        graknGraph.putEntityType("My-Type");

        lhs = graknGraph.graql().parsePattern("$x isa Your-Type");
        rhs = graknGraph.graql().parsePattern("$x isa My-Type");
        Rule rule = graknGraph.admin().getMetaRuleInference().putRule(lhs, rhs);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage("LHS", rule.getId(), rule.type().getLabel(), "Your-Type"));

        graknGraph.commit();
    }

    @Test
    public void whenCreatingRules_EnsureHypothesisAndConclusionTypesAreFilledOnCommit() throws InvalidGraphException{
        EntityType t1 = graknGraph.putEntityType("type1");
        EntityType t2 = graknGraph.putEntityType("type2");

        lhs = graknGraph.graql().parsePattern("$x isa type1");
        rhs = graknGraph.graql().parsePattern("$x isa type2");

        Rule rule = graknGraph.admin().getMetaRuleInference().putRule(lhs, rhs);
        assertThat(rule.getHypothesisTypes(), empty());
        assertThat(rule.getConclusionTypes(), empty());

        graknGraph.commit();

        assertThat(rule.getHypothesisTypes(), containsInAnyOrder(t1));
        assertThat(rule.getConclusionTypes(), containsInAnyOrder(t2));
    }

    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule(){
        graknGraph.putEntityType("type1");
        lhs = graknGraph.graql().parsePattern("$x isa type1");
        rhs = graknGraph.graql().parsePattern("$x isa type1");

        Rule rule1 = graknGraph.admin().getMetaRuleInference().putRule(lhs, rhs);
        Rule rule2 = graknGraph.admin().getMetaRuleInference().putRule(lhs, rhs);

        assertEquals(rule1, rule2);
    }

    @Ignore //This is ignored because we currently have no way to determine if patterns with different variables name are equivalent
    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithDifferentPatternVariables_ReturnTheSameRule(){
        graknGraph.putEntityType("type1");
        lhs = graknGraph.graql().parsePattern("$x isa type1");
        rhs = graknGraph.graql().parsePattern("$y isa type1");

        Rule rule1 = graknGraph.admin().getMetaRuleInference().putRule(lhs, rhs);
        Rule rule2 = graknGraph.admin().getMetaRuleInference().putRule(lhs, rhs);

        assertEquals(rule1, rule2);
    }

    @Test
    public void serializeTest() throws IOException, ClassNotFoundException {
        Thingy thingy = new Thingy(123, true);

        for (int i = 0; i < 100_000_000; i++) {
            Serializer.deserialize(Serializer.serialize(thingy));
//            Thingy.fromString(thingy.toString());
        }
    }


    static class Thingy implements Serializable {
        long a;
        boolean b;

        public Thingy() {}

        public Thingy(long a, boolean b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public String toString() {
            return (b ? "v" : "e") + a;
        }

        static Thingy fromString(String string) {
            long a = Long.parseLong(string.substring(1, string.length()));
            boolean b = string.substring(0, 1).equals("v");
            return new Thingy(a, b);
        }
    }

    public static class Serializer {

        static Kryo kryo = new Kryo();

        public static byte[] serialize(Thingy obj) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
            buffer.putLong(obj.a);
            buffer.put(obj.b ? (byte) 1 : (byte) 0);
            return buffer.array();
        }

        public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + 1);
            buffer.put(bytes);
            buffer.flip();//need flip
            long a = buffer.getLong();
            boolean b = buffer.get() == 1;
            return new Thingy(a, b);
        }

        public static byte[] serializeKryo(Object obj) throws IOException {
            try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
                try(Output o = new Output(b)){
                    kryo.writeObject(o, obj);
                }
                return b.toByteArray();
            }
        }

        public static Object deserializeKryo(byte[] bytes) throws IOException, ClassNotFoundException {
            try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
                try(Input o = new Input(b)){
                    return kryo.readObject(o, Thingy.class);
                }
            }
        }

    }
}