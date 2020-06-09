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
 */

package grakn.core.graql.query;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlDefine;
import graql.lang.query.MatchClause;
import graql.lang.statement.Statement;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class GraqlDefineIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    // TODO: migrate
    @Test
    public void testDefineSubRole() {
        tx.execute(Graql.define(
                type("marriage").sub(Graql.Token.Type.RELATION).relates("spouse1").relates("spouse2"),
                type("spouse").sub(Graql.Token.Type.ROLE),
                type("spouse1").sub("spouse"),
                type("spouse2").sub("spouse")
        ));

        assertExists(tx, type("spouse1"));
    }

    // TODO: should we allow variables in defines?
    @Test
    public void testReferenceByVariableNameAndTypeLabel() {
        tx.execute(Graql.define(
                var("abc").sub("entity"),
                var("abc").type("123"),
                type("123").plays("actor"),
                var("abc").plays("director")
        ));

        assertExists(tx, type("123").sub("entity"));
        assertExists(tx, type("123").plays("actor"));
        assertExists(tx, type("123").plays("director"));
    }

    // TODO: should we allow variables in defines?
    @Test
    public void whenExecutingADefineQuery_ResultContainsAllInsertedVars() {
        Statement type = var("type");
        Statement type2 = var("type2");

        // Note that two variables refer to the same type. They should both be in the result
        GraqlDefine query = Graql.define(type.type("my-type").sub("entity"), type2.type("my-type"));

        ConceptMap result = tx.execute(query).get(0);
        assertThat(result.vars(), containsInAnyOrder(type.var(), type2.var()));
        assertEquals(result.get(type.var()), result.get(type2.var()));
    }

    @Test
    public void whenDefiningARuleUsingParsedPatterns_ruleIsPersistedCorrectly() {
        Pattern when = Graql.parsePattern("$x isa entity;");
        Pattern then = Graql.parsePattern("$x isa entity;");
        Statement rule = type("my-rule").sub(Graql.Token.Type.RULE).when(when).then(then);
        tx.execute(Graql.define(rule));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void whenDefiningARuleUsingCoreAPI_ruleIsPersistedCorrectly(){
        tx.execute(Graql.define(type("good-movie").sub("movie")));
        GraqlDefine ruleDefinition = Graql.define(
                type("high-average-movies-are-good").sub("rule")
                        .when(
                                var("m").isa("movie").has("tmdb-vote-average", Graql.gte(7.5)))
                        .then(
                                var("m").isa("good-movie")
                )
        );
        tx.execute(ruleDefinition);
    }

    @Test
    public void whenDefiningAnOntologyConceptWithoutALabel_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("entity"), containsString("type")));
        tx.execute(Graql.define(var().sub("entity")));
    }

    @Test
    public void whenDefiningMetaType_Throw() {
        exception.expect(GraknConceptException.class);
        exception.expectMessage(ErrorMessage.INVALID_SUPER_TYPE.getMessage("my-metatype", Graql.Token.Type.THING));
        tx.execute(Graql.define(type("my-metatype").sub(Graql.Token.Type.THING)));
    }

    @Test
    public void whenDefiningAThing_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.ISA.toString()).getMessage());

        tx.execute(Graql.define(var("x").isa("movie")));
    }

    // TODO: migrate
    @Test
    public void whenModifyingAThingInADefineQuery_Throw() {
        ConceptId id = tx.getEntityType("movie").instances().iterator().next().id();

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(anyOf(
                is(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.HAS.toString()).getMessage()),
                is(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.VALUE.toString()).getMessage())
        ));

        tx.execute(Graql.define(var().id(id.getValue()).has("title", "Bob")));
    }

    // TODO: this is weird
    @Test @Ignore
    public void whenSpecifyingLabelOfAnExistingConcept_LabelIsChanged() {
        tx.putEntityType("a-new-type");

        EntityType type = tx.getEntityType("a-new-type");
        Label newLabel = Label.of("a-new-new-type");

        // TODO: figure out how this was possible in the first place
        //       how could we modify the label of a type by its ID????
        tx.execute(Graql.define(type(newLabel.getValue()).id(type.id().getValue())));

        assertEquals(newLabel, type.label());
    }

    @Test
    public void whenCallingToStringOfDefineQuery_ReturnCorrectRepresentation() {
        String queryString = "define my-entity sub entity;";
        GraqlDefine defineQuery = Graql.parse(queryString).asDefine();
        assertEquals(queryString, defineQuery.toString());
    }

    // TODO: migrate
    @Test
    public void whenDefiningARelation_SubRoleCasUseAs() {
        tx.execute(Graql.define(type("parentship").sub(Graql.Token.Type.RELATION)
                          .relates("parent")
                          .relates("child")));
        tx.execute(Graql.define(type("fatherhood").sub(Graql.Token.Type.RELATION)
                          .relates("father", "parent")
                          .relates("son", "child")));

        RelationType marriage = tx.getRelationType("fatherhood");
        Role father = tx.getRole("father");
        Role son = tx.getRole("son");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(father, son));
        assertEquals(tx.getRole("parent"), father.sup());
        assertEquals(tx.getRole("child"), son.sup());
    }

    private boolean schemaObjectsExist(Statement... vars){
        boolean exist = true;
        try {
            for (Statement var : vars) {
                exist = !tx.execute(Graql.match(var)).isEmpty();
                if (!exist) break;
            }
        } catch(GraqlSemanticException e){
            exist = false;
        }
        return exist;
    }

    private void assertDefine(Statement... vars) {
        // Make sure vars don't exist
        assertFalse(schemaObjectsExist(vars));

        // Define all vars
        tx.execute(Graql.define(vars));

        // Make sure all vars exist
        for (Statement var : vars) {
            assertExists(tx, var);
        }

        // Undefine all vars
        tx.execute(Graql.undefine(vars));

        // Make sure vars don't exist
        assertFalse(schemaObjectsExist(vars));
    }
}
