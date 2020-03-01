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
import grakn.core.rule.GraknTestServer;
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

import static grakn.core.core.Schema.ImplicitType.HAS;
import static grakn.core.core.Schema.ImplicitType.HAS_OWNER;
import static grakn.core.core.Schema.ImplicitType.HAS_VALUE;
import static grakn.core.core.Schema.ImplicitType.KEY;
import static grakn.core.core.Schema.ImplicitType.KEY_OWNER;
import static grakn.core.core.Schema.ImplicitType.KEY_VALUE;
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
        tx = session.writeTransaction();
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
    }

    @Test
    public void testDefineSub() {
        assertDefine(var("x").type("cool-movie").sub("movie"));
    }

    @Test
    public void testDefineSchema() {
        tx.execute(Graql.define(
                type("pokemon").sub(Graql.Token.Type.ENTITY),
                type("evolution").sub(Graql.Token.Type.RELATION),
                type("evolves-from").sub(Graql.Token.Type.ROLE),
                type("evolves-to").sub(Graql.Token.Type.ROLE),
                type("evolution").relates("evolves-from").relates("evolves-to"),
                type("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        ));

        assertExists(tx, type("pokemon").sub(Graql.Token.Type.ENTITY));
        assertExists(tx, type("evolution").sub(Graql.Token.Type.RELATION));
        assertExists(tx, type("evolves-from").sub(Graql.Token.Type.ROLE));
        assertExists(tx, type("evolves-to").sub(Graql.Token.Type.ROLE));
        assertExists(tx, type("evolution").relates("evolves-from").relates("evolves-to"));
        assertExists(tx, type("pokemon").plays("evolves-from").plays("evolves-to"));
    }

    @Test
    public void testDefineIsAbstract() {
        tx.execute(Graql.define(
                type("concrete-type").sub(Graql.Token.Type.ENTITY),
                type("abstract-type").isAbstract().sub(Graql.Token.Type.ENTITY)
        ));

        assertNotExists(tx, type("concrete-type").isAbstract());
        assertExists(tx, type("abstract-type").isAbstract());
    }

    @Test
    public void testDefineDataType() {
        tx.execute(Graql.define(
                type("my-type").sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.LONG)
        ));

        MatchClause match = Graql.match(var("x").type("my-type"));
        AttributeType.DataType datatype = tx.stream(match).iterator().next().get("x").asAttributeType().dataType();

        Assert.assertEquals(AttributeType.DataType.LONG, datatype);
    }

    @Test
    public void testDefineSubResourceType() {
        tx.execute(Graql.define(
                type("my-type").sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.STRING),
                type("sub-type").sub("my-type")
        ));

        MatchClause match = Graql.match(var("x").type("sub-type"));
        AttributeType.DataType datatype = tx.stream(match).iterator().next().get("x").asAttributeType().dataType();

        Assert.assertEquals(AttributeType.DataType.STRING, datatype);
    }

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

    @Test
    public void testDefineReferenceByName() {
        String roleTypeLabel = HAS_OWNER.getLabel("title").getValue();
        tx.execute(Graql.define(
                type("new-type").sub(Graql.Token.Type.ENTITY),
                type("new-type").plays(roleTypeLabel)
        ));

        tx.execute(Graql.insert(var("x").isa("new-type")));

        MatchClause typeQuery = Graql.match(var("n").type("new-type"));

        assertEquals(1, tx.stream(typeQuery).count());

        // We checked count ahead of time
        //noinspection OptionalGetWithoutIsPresent
        EntityType newType = tx.stream(typeQuery.get("n")).map(ans -> ans.get("n")).findFirst().get().asEntityType();

        assertTrue(newType.playing().anyMatch(role -> role.equals(tx.getRole(roleTypeLabel))));

        assertExists(tx, var().isa("new-type"));
    }

    @Test
    public void testHas() {
        String resourceType = "a-new-resource-type";

        tx.execute(Graql.define(
                type("a-new-type").sub("entity").has(resourceType),
                type(resourceType).sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.STRING),
                type("an-unconnected-resource-type").sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.LONG)
        ));

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertExists(tx, type("a-new-type").sub("entity").has(resourceType));
        assertNotExists(tx, type("a-new-type").has("title"));
        assertNotExists(tx, type("movie").has(resourceType));
        assertNotExists(tx, type("a-new-type").has("an-unconnected-resource-type"));

        Statement hasResource = type(HAS.getLabel(resourceType).getValue());
        Statement hasResourceOwner = type(HAS_OWNER.getLabel(resourceType).getValue());
        Statement hasResourceValue = type(HAS_VALUE.getLabel(resourceType).getValue());

        // Make sure the expected ontology elements are created
        assertExists(tx, hasResource.sub(Graql.Token.Type.RELATION));
        assertExists(tx, hasResourceOwner.sub(Graql.Token.Type.ROLE));
        assertExists(tx, hasResourceValue.sub(Graql.Token.Type.ROLE));
        assertExists(tx, hasResource.relates(hasResourceOwner));
        assertExists(tx, hasResource.relates(hasResourceValue));
        assertExists(tx, type("a-new-type").plays(hasResourceOwner));
        assertExists(tx, type(resourceType).plays(hasResourceValue));
    }

    @Test
    public void whenDefiningAKey_appropriateSchemaConceptsAreCreated() {
        String resourceType = "a-new-resource-type";

        tx.execute(Graql.define(
                type("a-new-type").sub("entity").key(resourceType),
                type(resourceType).sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.STRING)
        ));

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertExists(tx, type("a-new-type").sub("entity").key(resourceType));
        assertExists(tx, type("a-new-type").sub("entity").has(resourceType));
        assertNotExists(tx, type("a-new-type").sub("entity").key("title"));
        assertNotExists(tx, type("movie").sub("entity").key(resourceType));

        Statement key = type(KEY.getLabel(resourceType).getValue());
        Statement keyOwner = type(KEY_OWNER.getLabel(resourceType).getValue());
        Statement keyValue = type(KEY_VALUE.getLabel(resourceType).getValue());

        // Make sure the expected ontology elements are created
        assertExists(tx, key.sub(Graql.Token.Type.RELATION));
        assertExists(tx, keyOwner.sub(Graql.Token.Type.ROLE));
        assertExists(tx, keyValue.sub(Graql.Token.Type.ROLE));
        assertExists(tx, key.relates(keyOwner));
        assertExists(tx, key.relates(keyValue));
        assertExists(tx, type("a-new-type").plays(keyOwner));
        assertExists(tx, type(resourceType).plays(keyValue));
    }

    @Test
    public void whenDefiningResourceTypeWithRegex_regexIsAppliedCorrectly() {
        tx.execute(Graql.define(type("greeting").sub(Graql.Token.Type.ATTRIBUTE).datatype(Graql.Token.DataType.STRING).regex("hello|good day")));

        MatchClause match = Graql.match(var("x").type("greeting"));
        assertEquals("hello|good day", tx.stream(match.get("x")).map(ans -> ans.get("x")).findFirst().get().asAttributeType().regex());
    }

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
    public void whenChangingTheSuperOfAnExistingConcept_ApplyTheChange() {
        EntityType newType = tx.putEntityType("a-new-type");
        EntityType movie = tx.getEntityType("movie");

        tx.execute(Graql.define(type("a-new-type").sub("movie")));

        assertEquals(movie, newType.sup());
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
    public void whenDefiningAttributeTypeWithoutDataType_weThrow() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        tx.execute(Graql.define(type("my-resource").sub(Graql.Token.Type.ATTRIBUTE)));
    }

    @Test
    public void whenDefiningRecursiveTypes_weThrow() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        tx.execute(Graql.define(type("thingy").sub("thingy")));
    }

    @Test
    public void whenDefiningAnOntologyConceptWithoutALabel_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("entity"), containsString("type")));
        tx.execute(Graql.define(var().sub("entity")));
    }

    @Test
    public void whenATypeHasNonExistentResource_weThrow() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage("nothing");
        tx.execute(Graql.define(type("blah this").sub("entity").has("nothing")));
    }

    @Test
    public void whenDefiningMetaType_Throw() {
        exception.expect(GraknConceptException.class);
        exception.expectMessage(ErrorMessage.INVALID_SUPER_TYPE.getMessage("my-metatype", Graql.Token.Type.THING));
        tx.execute(Graql.define(type("my-metatype").sub(Graql.Token.Type.THING)));
    }

    @Test
    public void whenSpecifyingExistingTypeWithIncorrectDataType_Throw() {
        AttributeType name = tx.getAttributeType("name");

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(
                GraqlSemanticException.insertPropertyOnExistingConcept("datatype", AttributeType.DataType.BOOLEAN, name).getMessage()
        );

        tx.execute(Graql.define(type("name").datatype(Graql.Token.DataType.BOOLEAN)));
    }

    @Test
    public void whenSpecifyingDataTypeOnAnEntityType_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(
                allOf(containsString("Unexpected property"), containsString("datatype"), containsString("my-type"))
        );

        tx.execute(Graql.define(type("my-type").sub("entity").datatype(Graql.Token.DataType.BOOLEAN)));
    }

    @Test
    public void whenDefiningRuleWithoutWhen_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("when")));
        tx.execute(Graql.define(type("a-rule").sub(Graql.Token.Type.RULE).then(var("x").isa("movie"))));
    }

    @Test
    public void whenDefiningRuleWithoutThen_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("then")));
        tx.execute(Graql.define(type("a-rule").sub(Graql.Token.Type.RULE).when(var("x").isa("movie"))));
    }

    @Test
    public void whenDefiningANonRuleWithAWhenPattern_Throw() {
        Statement rule = type("yes").sub(Graql.Token.Type.ENTITY).when(var("x").isa("yes"));

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(anyOf(
                // Either we see "entity" and an unexpected "when"...
                allOf(containsString("unexpected property"), containsString("when")),
                // ...or we see "when" and don't find the expected "then"
                containsString(GraqlSemanticException.insertNoExpectedProperty("then", rule).getMessage()))
        );

        tx.execute(Graql.define(rule));
    }

    @Test
    public void whenDefiningANonRuleWithAThenPattern_Throw() {
        Statement rule = type("some-type").sub(Graql.Token.Type.ENTITY).then(var("x").isa("some-type"));

        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(anyOf(
                // Either we see "entity" and an unexpected "when"...
                allOf(containsString("unexpected property"), containsString("then")),
                // ...or we see "when" and don't find the expected "then"
                containsString(GraqlSemanticException.insertNoExpectedProperty("when", rule).getMessage()))
        );

        tx.execute(Graql.define(rule));
    }

    @Test
    public void whenDefiningAThing_Throw() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(GraqlSemanticException.defineUnsupportedProperty(Graql.Token.Property.ISA.toString()).getMessage());

        tx.execute(Graql.define(var("x").isa("movie")));
    }

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

    @Test
    public void whenDefiningARelation_SubRoleDeclarationsCanBeSkipped() {
        tx.execute(Graql.define(type("marriage").sub(Graql.Token.Type.RELATION).relates("husband").relates("wife")));

        RelationType marriage = tx.getRelationType("marriage");
        Role husband = tx.getRole("husband");
        Role wife = tx.getRole("wife");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(husband, wife));
    }

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

    @Test
    public void whenDefiningARule_SubRuleDeclarationsCanBeSkipped() {
        Pattern when = Graql.parsePattern("$x isa entity;");
        Pattern then = Graql.parsePattern("$x isa entity;");
        Statement vars = type("my-rule").when(when).then(then);
        tx.execute(Graql.define(vars));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void whenDefiningARelation_SubRoleDeclarationsCanBeSkipped_EvenWhenRoleInReferredToInOtherContexts() {
        tx.execute(Graql.define(
                type("marriage").sub(Graql.Token.Type.RELATION).relates("husband").relates("wife"),
                type("person").plays("husband").plays("wife")
        ));

        RelationType marriage = tx.getRelationType("marriage");
        EntityType person = tx.getEntityType("person");
        Role husband = tx.getRole("husband");
        Role wife = tx.getRole("wife");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(husband, wife));
        assertThat(person.playing().toArray(), hasItemInArray(wife));
        assertThat(person.playing().toArray(), hasItemInArray(husband));
    }

    @Test
    public void whenDefiningARelationWithNonRoles_Throw() {
        exception.expect(GraknException.class);

        tx.execute(Graql.define(
                type("marriage").sub(Graql.Token.Type.RELATION).relates("husband").relates("wife"),
                type("wife").sub(Graql.Token.Type.ENTITY)
        ));
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
