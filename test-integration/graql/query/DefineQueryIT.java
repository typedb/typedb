/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Transaction;
import grakn.core.server.exception.InvalidKBException;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static grakn.core.graql.internal.Schema.ImplicitType.HAS;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.HAS_VALUE;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_OWNER;
import static grakn.core.graql.internal.Schema.ImplicitType.KEY_VALUE;
import static grakn.core.graql.internal.Schema.MetaSchema.ENTITY;
import static grakn.core.graql.internal.Schema.MetaSchema.RELATIONSHIP;
import static grakn.core.graql.internal.Schema.MetaSchema.ROLE;
import static grakn.core.graql.internal.Schema.MetaSchema.RULE;
import static grakn.core.graql.query.Graql.parse;
import static grakn.core.graql.query.Graql.type;
import static grakn.core.graql.query.Graql.var;
import static grakn.core.util.GraqlTestUtil.assertExists;
import static grakn.core.util.GraqlTestUtil.assertNotExists;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class DefineQueryIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final GraknTestServer graknServer = new GraknTestServer();
    public static SessionImpl session;
    private TransactionOLTP tx;

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

    @Test
    public void testDefineSub() {
        assertDefine(var("x").type("cool-movie").sub("movie"));
    }

    @Test
    public void testDefineSchema() {
        tx.execute(Graql.define(
                Graql.type("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                Graql.type("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                Graql.type("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                Graql.type("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                Graql.type("evolution").relates("evolves-from").relates("evolves-to"),
                Graql.type("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        ));

        assertExists(tx, Graql.type("pokemon").sub(ENTITY.getLabel().getValue()));
        assertExists(tx, Graql.type("evolution").sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(tx, Graql.type("evolves-from").sub(ROLE.getLabel().getValue()));
        assertExists(tx, Graql.type("evolves-to").sub(ROLE.getLabel().getValue()));
        assertExists(tx, Graql.type("evolution").relates("evolves-from").relates("evolves-to"));
        assertExists(tx, Graql.type("pokemon").plays("evolves-from").plays("evolves-to"));
    }

    @Test
    public void testDefineIsAbstract() {
        tx.execute(Graql.define(
                Graql.type("concrete-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                Graql.type("abstract-type").isAbstract().sub(Schema.MetaSchema.ENTITY.getLabel().getValue())
        ));

        assertNotExists(tx, Graql.type("concrete-type").isAbstract());
        assertExists(tx, Graql.type("abstract-type").isAbstract());
    }

    @Test
    public void testDefineDataType() {
        tx.execute(Graql.define(
                Graql.type("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.LONG)
        ));

        MatchClause match = Graql.match(var("x").type("my-type"));
        AttributeType.DataType datatype = tx.stream(match).iterator().next().get("x").asAttributeType().dataType();

        Assert.assertEquals(AttributeType.DataType.LONG, datatype);
    }

    @Test
    public void testDefineSubResourceType() {
        tx.execute(Graql.define(
                Graql.type("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.STRING),
                Graql.type("sub-type").sub("my-type")
        ));

        MatchClause match = Graql.match(var("x").type("sub-type"));
        AttributeType.DataType datatype = tx.stream(match).iterator().next().get("x").asAttributeType().dataType();

        Assert.assertEquals(AttributeType.DataType.STRING, datatype);
    }

    @Test
    public void testDefineSubRole() {
        tx.execute(Graql.define(
                Graql.type("marriage").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()).relates("spouse1").relates("spouse2"),
                Graql.type("spouse").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                Graql.type("spouse1").sub("spouse"),
                Graql.type("spouse2").sub("spouse")
        ));

        assertExists(tx, Graql.type("spouse1"));
    }

    @Test
    public void testReferenceByVariableNameAndTypeLabel() {
        tx.execute(Graql.define(
                var("abc").sub("entity"),
                var("abc").type("123"),
                Graql.type("123").plays("actor"),
                var("abc").plays("director")
        ));

        assertExists(tx, Graql.type("123").sub("entity"));
        assertExists(tx, Graql.type("123").plays("actor"));
        assertExists(tx, Graql.type("123").plays("director"));
    }

    @Test
    public void testDefineReferenceByName() {
        String roleTypeLabel = HAS_OWNER.getLabel("title").getValue();
        tx.execute(Graql.define(
                Graql.type("new-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                Graql.type("new-type").plays(roleTypeLabel)
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
                Graql.type("a-new-type").sub("entity").has(resourceType),
                Graql.type(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.STRING),
                Graql.type("an-unconnected-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.LONG)
        ));

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertExists(tx, Graql.type("a-new-type").sub("entity").has(resourceType));
        assertNotExists(tx, Graql.type("a-new-type").has("title"));
        assertNotExists(tx, Graql.type("movie").has(resourceType));
        assertNotExists(tx, Graql.type("a-new-type").has("an-unconnected-resource-type"));

        Statement hasResource = type(HAS.getLabel(resourceType).getValue());
        Statement hasResourceOwner = type(HAS_OWNER.getLabel(resourceType).getValue());
        Statement hasResourceValue = type(HAS_VALUE.getLabel(resourceType).getValue());

        // Make sure the expected ontology elements are created
        assertExists(tx, hasResource.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(tx, hasResourceOwner.sub(ROLE.getLabel().getValue()));
        assertExists(tx, hasResourceValue.sub(ROLE.getLabel().getValue()));
        assertExists(tx, hasResource.relates(hasResourceOwner));
        assertExists(tx, hasResource.relates(hasResourceValue));
        assertExists(tx, Graql.type("a-new-type").plays(hasResourceOwner));
        assertExists(tx, Graql.type(resourceType).plays(hasResourceValue));
    }

    @Test
    public void testKey() {
        String resourceType = "a-new-resource-type";

        tx.execute(Graql.define(
                Graql.type("a-new-type").sub("entity").key(resourceType),
                Graql.type(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.STRING)
        ));

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertExists(tx, Graql.type("a-new-type").sub("entity").key(resourceType));
        assertExists(tx, Graql.type("a-new-type").sub("entity").has(resourceType));
        assertNotExists(tx, Graql.type("a-new-type").sub("entity").key("title"));
        assertNotExists(tx, Graql.type("movie").sub("entity").key(resourceType));

        Statement key = type(KEY.getLabel(resourceType).getValue());
        Statement keyOwner = type(KEY_OWNER.getLabel(resourceType).getValue());
        Statement keyValue = type(KEY_VALUE.getLabel(resourceType).getValue());

        // Make sure the expected ontology elements are created
        assertExists(tx, key.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(tx, keyOwner.sub(ROLE.getLabel().getValue()));
        assertExists(tx, keyValue.sub(ROLE.getLabel().getValue()));
        assertExists(tx, key.relates(keyOwner));
        assertExists(tx, key.relates(keyValue));
        assertExists(tx, Graql.type("a-new-type").plays(keyOwner));
        assertExists(tx, Graql.type(resourceType).plays(keyValue));
    }

    @Test
    public void testResourceTypeRegex() {
        tx.execute(Graql.define(Graql.type("greeting").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(Query.DataType.STRING).like("hello|good day")));

        MatchClause match = Graql.match(var("x").type("greeting"));
        assertEquals("hello|good day", tx.stream(match.get("x")).map(ans -> ans.get("x")).findFirst().get().asAttributeType().regex());
    }

    @Test
    public void whenExecutingADefineQuery_ResultContainsAllInsertedVars() {
        Statement typeVar = var("type");
        Statement type2Var = var("type2");

        // Note that two variables refer to the same type. They should both be in the result
        DefineQuery query = Graql.define(typeVar.type("my-type").sub("entity"), type2Var.type("my-type"));

        ConceptMap result = tx.execute(query).get(0);
        assertThat(result.vars(), containsInAnyOrder(typeVar.var(), type2Var.var()));
        assertEquals(result.get(typeVar.var()), result.get(type2Var.var()));
    }

    @Test
    public void whenChangingTheSuperOfAnExistingConcept_ApplyTheChange() {
        EntityType newType = tx.putEntityType("a-new-type");
        EntityType movie = tx.getEntityType("movie");

        tx.execute(Graql.define(Graql.type("a-new-type").sub("movie")));

        assertEquals(movie, newType.sup());
    }

    @Test
    public void whenDefiningARule_TheRuleIsInTheKB() {
        Pattern when = Graql.parsePattern("$x isa entity");
        Pattern then = Graql.parsePattern("$x isa entity");
        Statement vars = Graql.type("my-rule").sub(type(RULE.getLabel().getValue())).when(when).then(then);
        tx.execute(Graql.define(vars));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void testErrorResourceTypeWithoutDataType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        tx.execute(Graql.define(Graql.type("my-resource").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue())));
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        tx.execute(Graql.define(Graql.type("thingy").sub("thingy")));
    }

    @Test
    public void whenDefiningAnOntologyConceptWithoutALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("entity"), containsString("label")));
        tx.execute(Graql.define(var().sub("entity")));
    }

    @Test
    public void testErrorWhenNonExistentResource() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("nothing");
        tx.execute(Graql.define(Graql.type("blah this").sub("entity").has("nothing")));
    }

    @Test
    public void whenDefiningMetaType_Throw() {
        exception.expect(InvalidKBException.class);
        exception.expectMessage(ErrorMessage.INSERT_METATYPE.getMessage("my-metatype", Schema.MetaSchema.THING.getLabel().getValue()));
        tx.execute(Graql.define(Graql.type("my-metatype").sub(Schema.MetaSchema.THING.getLabel().getValue())));
    }

    @Test
    public void whenSpecifyingExistingTypeWithIncorrectDataType_Throw() {
        AttributeType name = tx.getAttributeType("name");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                GraqlQueryException.insertPropertyOnExistingConcept("datatype", AttributeType.DataType.BOOLEAN, name).getMessage()
        );

        tx.execute(Graql.define(Graql.type("name").datatype(Query.DataType.BOOLEAN)));
    }

    @Test
    public void whenSpecifyingDataTypeOnAnEntityType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("unexpected property"), containsString("datatype"), containsString("my-type"))
        );

        tx.execute(Graql.define(Graql.type("my-type").sub("entity").datatype(Query.DataType.BOOLEAN)));
    }

    @Test
    public void whenDefiningRuleWithoutWhen_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("when")));
        tx.execute(Graql.define(Graql.type("a-rule").sub(type(RULE.getLabel().getValue())).then(var("x").isa("movie"))));
    }

    @Test
    public void whenDefiningRuleWithoutThen_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("then")));
        tx.execute(Graql.define(Graql.type("a-rule").sub(type(RULE.getLabel().getValue())).when(var("x").isa("movie"))));
    }

    @Test
    public void whenDefiningANonRuleWithAWhenPattern_Throw() {
        Statement rule = Graql.type("yes").sub(type(ENTITY.getLabel().getValue())).when(var("x"));

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                // Either we see "entity" and an unexpected "when"...
                allOf(containsString("unexpected property"), containsString("when")),
                // ...or we see "when" and don't find the expected "then"
                containsString(GraqlQueryException.insertNoExpectedProperty("then", rule).getMessage()))
        );

        tx.execute(Graql.define(rule));
    }

    @Test
    public void whenDefiningANonRuleWithAThenPattern_Throw() {
        Statement rule = Graql.type("covfefe").sub(type(ENTITY.getLabel().getValue())).then(var("x"));

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                // Either we see "entity" and an unexpected "when"...
                allOf(containsString("unexpected property"), containsString("then")),
                // ...or we see "when" and don't find the expected "then"
                containsString(GraqlQueryException.insertNoExpectedProperty("when", rule).getMessage()))
        );

        tx.execute(Graql.define(rule));
    }

    @Test
    public void whenDefiningAThing_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.defineUnsupportedProperty(Query.Property.ISA.toString()).getMessage());

        tx.execute(Graql.define(var("x").isa("movie")));
    }

    @Test
    public void whenModifyingAThingInADefineQuery_Throw() {
        ConceptId id = tx.getEntityType("movie").instances().iterator().next().id();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                is(GraqlQueryException.defineUnsupportedProperty(Query.Property.HAS.toString()).getMessage()),
                is(GraqlQueryException.defineUnsupportedProperty(Query.Property.VALUE.toString()).getMessage())
        ));

        tx.execute(Graql.define(var().id(id.getValue()).has("title", "Bob")));
    }

    @Test
    public void whenSpecifyingLabelOfAnExistingConcept_LabelIsChanged() {
        tx.putEntityType("a-new-type");

        EntityType type = tx.getEntityType("a-new-type");
        Label newLabel = Label.of("a-new-new-type");

        tx.execute(Graql.define(type(newLabel.getValue()).id(type.id().getValue())));

        assertEquals(newLabel, type.label());
    }

    @Test
    public void whenCallingToStringOfDefineQuery_ReturnCorrectRepresentation() {
        String queryString = "define label my-entity sub entity;";
        DefineQuery defineQuery = parse(queryString);
        assertEquals(queryString, defineQuery.toString());
    }

    @Test
    public void whenDefiningARelationship_SubRoleDeclarationsCanBeSkipped() {
        tx.execute(Graql.define(Graql.type("marriage").sub(type(RELATIONSHIP.getLabel().getValue())).relates("husband").relates("wife")));

        RelationType marriage = tx.getRelationshipType("marriage");
        Role husband = tx.getRole("husband");
        Role wife = tx.getRole("wife");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(husband, wife));
    }

    @Test
    public void whenDefiningARelationship_SubRoleCasUseAs() {
        tx.execute(Graql.define(Graql.type("parentship").sub(type(RELATIONSHIP.getLabel().getValue()))
                          .relates("parent")
                          .relates("child")));
        tx.execute(Graql.define(Graql.type("fatherhood").sub(type(RELATIONSHIP.getLabel().getValue()))
                          .relates("father", "parent")
                          .relates("son", "child")));

        RelationType marriage = tx.getRelationshipType("fatherhood");
        Role father = tx.getRole("father");
        Role son = tx.getRole("son");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(father, son));
        assertEquals(tx.getRole("parent"), father.sup());
        assertEquals(tx.getRole("child"), son.sup());
    }

    @Test
    public void whenDefiningARule_SubRuleDeclarationsCanBeSkipped() {
        Pattern when = Graql.parsePattern("$x isa entity");
        Pattern then = Graql.parsePattern("$x isa entity");
        Statement vars = Graql.type("my-rule").when(when).then(then);
        tx.execute(Graql.define(vars));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void whenDefiningARelationship_SubRoleDeclarationsCanBeSkipped_EvenWhenRoleInReferredToInOtherContexts() {
        tx.execute(Graql.define(
                Graql.type("marriage").sub(type(RELATIONSHIP.getLabel().getValue())).relates("husband").relates("wife"),
                Graql.type("person").plays("husband").plays("wife")
        ));

        RelationType marriage = tx.getRelationshipType("marriage");
        EntityType person = tx.getEntityType("person");
        Role husband = tx.getRole("husband");
        Role wife = tx.getRole("wife");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(husband, wife));
        assertThat(person.playing().toArray(), hasItemInArray(wife));
        assertThat(person.playing().toArray(), hasItemInArray(husband));
    }

    @Test
    public void whenDefiningARelationshipWithNonRoles_Throw() {
        exception.expect(GraknException.class);

        tx.execute(Graql.define(
                Graql.type("marriage").sub(type(RELATIONSHIP.getLabel().getValue())).relates("husband").relates("wife"),
                Graql.type("wife").sub(type(ENTITY.getLabel().getValue()))
        ));
    }

    private void assertDefine(Statement... vars) {
        // Make sure vars don't exist
        for (Statement var : vars) {
            assertNotExists(tx, var);
        }

        // Define all vars
        tx.execute(Graql.define(vars));

        // Make sure all vars exist
        for (Statement var : vars) {
            assertExists(tx, var);
        }

        // Undefine all vars
        tx.execute(Graql.undefine(vars));

        // Make sure vars don't exist
        for (Statement var : vars) {
            assertNotExists(tx, var);
        }
    }
}
