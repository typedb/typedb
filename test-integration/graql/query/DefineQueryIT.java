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
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
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

import static grakn.core.graql.concept.AttributeType.DataType.BOOLEAN;
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
import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
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
        assertDefine(var("x").label("cool-movie").sub("movie"));
    }

    @Test
    public void testDefineSchema() {
        tx.execute(Graql.define(
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        ));

        assertExists(tx, label("pokemon").sub(ENTITY.getLabel().getValue()));
        assertExists(tx, label("evolution").sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(tx, label("evolves-from").sub(ROLE.getLabel().getValue()));
        assertExists(tx, label("evolves-to").sub(ROLE.getLabel().getValue()));
        assertExists(tx, label("evolution").relates("evolves-from").relates("evolves-to"));
        assertExists(tx, label("pokemon").plays("evolves-from").plays("evolves-to"));
    }

    @Test
    public void testDefineIsAbstract() {
        tx.execute(Graql.define(
                label("concrete-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("abstract-type").isAbstract().sub(Schema.MetaSchema.ENTITY.getLabel().getValue())
        ));

        assertNotExists(tx, label("concrete-type").isAbstract());
        assertExists(tx, label("abstract-type").isAbstract());
    }

    @Test
    public void testDefineDataType() {
        tx.execute(Graql.define(
                label("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.LONG)
        ));

        MatchClause match = Graql.match(var("x").label("my-type"));
        AttributeType.DataType datatype = tx.stream(match).iterator().next().get("x").asAttributeType().dataType();

        Assert.assertEquals(AttributeType.DataType.LONG, datatype);
    }

    @Test
    public void testDefineSubResourceType() {
        tx.execute(Graql.define(
                label("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                label("sub-type").sub("my-type")
        ));

        MatchClause match = Graql.match(var("x").label("sub-type"));
        AttributeType.DataType datatype = tx.stream(match).iterator().next().get("x").asAttributeType().dataType();

        Assert.assertEquals(AttributeType.DataType.STRING, datatype);
    }

    @Test
    public void testDefineSubRole() {
        tx.execute(Graql.define(
                label("marriage").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()).relates("spouse1").relates("spouse2"),
                label("spouse").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("spouse1").sub("spouse"),
                label("spouse2").sub("spouse")
        ));

        assertExists(tx, label("spouse1"));
    }

    @Test
    public void testReferenceByVariableNameAndTypeLabel() {
        tx.execute(Graql.define(
                var("abc").sub("entity"),
                var("abc").label("123"),
                label("123").plays("actor"),
                var("abc").plays("director")
        ));

        assertExists(tx, label("123").sub("entity"));
        assertExists(tx, label("123").plays("actor"));
        assertExists(tx, label("123").plays("director"));
    }

    @Test
    public void testDefineReferenceByName() {
        String roleTypeLabel = HAS_OWNER.getLabel("title").getValue();
        tx.execute(Graql.define(
                label("new-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("new-type").plays(roleTypeLabel)
        ));

        tx.execute(Graql.insert(var("x").isa("new-type")));

        MatchClause typeQuery = Graql.match(var("n").label("new-type"));

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
                label("a-new-type").sub("entity").has(resourceType),
                label(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                label("an-unconnected-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.LONG)
        ));

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertExists(tx, label("a-new-type").sub("entity").has(resourceType));
        assertNotExists(tx, label("a-new-type").has("title"));
        assertNotExists(tx, label("movie").has(resourceType));
        assertNotExists(tx, label("a-new-type").has("an-unconnected-resource-type"));

        Statement hasResource = label(HAS.getLabel(resourceType));
        Statement hasResourceOwner = label(HAS_OWNER.getLabel(resourceType));
        Statement hasResourceValue = label(HAS_VALUE.getLabel(resourceType));

        // Make sure the expected ontology elements are created
        assertExists(tx, hasResource.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(tx, hasResourceOwner.sub(ROLE.getLabel().getValue()));
        assertExists(tx, hasResourceValue.sub(ROLE.getLabel().getValue()));
        assertExists(tx, hasResource.relates(hasResourceOwner));
        assertExists(tx, hasResource.relates(hasResourceValue));
        assertExists(tx, label("a-new-type").plays(hasResourceOwner));
        assertExists(tx, label(resourceType).plays(hasResourceValue));
    }

    @Test
    public void testKey() {
        String resourceType = "a-new-resource-type";

        tx.execute(Graql.define(
                label("a-new-type").sub("entity").key(resourceType),
                label(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ));

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertExists(tx, label("a-new-type").sub("entity").key(resourceType));
        assertExists(tx, label("a-new-type").sub("entity").has(resourceType));
        assertNotExists(tx, label("a-new-type").sub("entity").key("title"));
        assertNotExists(tx, label("movie").sub("entity").key(resourceType));

        Statement key = label(KEY.getLabel(resourceType));
        Statement keyOwner = label(KEY_OWNER.getLabel(resourceType));
        Statement keyValue = label(KEY_VALUE.getLabel(resourceType));

        // Make sure the expected ontology elements are created
        assertExists(tx, key.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(tx, keyOwner.sub(ROLE.getLabel().getValue()));
        assertExists(tx, keyValue.sub(ROLE.getLabel().getValue()));
        assertExists(tx, key.relates(keyOwner));
        assertExists(tx, key.relates(keyValue));
        assertExists(tx, label("a-new-type").plays(keyOwner));
        assertExists(tx, label(resourceType).plays(keyValue));
    }

    @Test
    public void testResourceTypeRegex() {
        tx.execute(Graql.define(label("greeting").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING).regex("hello|good day")));

        MatchClause match = Graql.match(var("x").label("greeting"));
        assertEquals("hello|good day", tx.stream(match.get("x")).map(ans -> ans.get("x")).findFirst().get().asAttributeType().regex());
    }

    @Test
    public void whenExecutingADefineQuery_ResultContainsAllInsertedVars() {
        Variable type = var("type");
        Variable type2 = var("type2");

        // Note that two variables refer to the same type. They should both be in the result
        DefineQuery query = Graql.define(type.label("my-type").sub("entity"), type2.label("my-type"));

        ConceptMap result = tx.execute(query).get(0);
        assertThat(result.vars(), containsInAnyOrder(type, type2));
        assertEquals(result.get(type), result.get(type2));
    }

    @Test
    public void whenChangingTheSuperOfAnExistingConcept_ApplyTheChange() {
        EntityType newType = tx.putEntityType("a-new-type");
        EntityType movie = tx.getEntityType("movie");

        tx.execute(Graql.define(label("a-new-type").sub("movie")));

        assertEquals(movie, newType.sup());
    }

    @Test
    public void whenDefiningARule_TheRuleIsInTheKB() {
        Pattern when = Pattern.parse("$x isa entity");
        Pattern then = Pattern.parse("$x isa entity");
        Statement vars = label("my-rule").sub(label(RULE.getLabel())).when(when).then(then);
        tx.execute(Graql.define(vars));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void testErrorResourceTypeWithoutDataType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        tx.execute(Graql.define(label("my-resource").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue())));
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        tx.execute(Graql.define(label("thingy").sub("thingy")));
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
        tx.execute(Graql.define(label("blah this").sub("entity").has("nothing")));
    }

    @Test
    public void whenDefiningMetaType_Throw() {
        exception.expect(InvalidKBException.class);
        exception.expectMessage(ErrorMessage.INSERT_METATYPE.getMessage("my-metatype", Schema.MetaSchema.THING.getLabel().getValue()));
        tx.execute(Graql.define(label("my-metatype").sub(Schema.MetaSchema.THING.getLabel().getValue())));
    }

    @Test
    public void whenSpecifyingExistingTypeWithIncorrectDataType_Throw() {
        AttributeType name = tx.getAttributeType("name");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                GraqlQueryException.insertPropertyOnExistingConcept("datatype", BOOLEAN, name).getMessage()
        );

        tx.execute(Graql.define(label("name").datatype(BOOLEAN)));
    }

    @Test
    public void whenSpecifyingDataTypeOnAnEntityType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("unexpected property"), containsString("datatype"), containsString("my-type"))
        );

        tx.execute(Graql.define(label("my-type").sub("entity").datatype(BOOLEAN)));
    }

    @Test
    public void whenDefiningRuleWithoutWhen_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("when")));
        tx.execute(Graql.define(label("a-rule").sub(label(RULE.getLabel())).then(var("x").isa("movie"))));
    }

    @Test
    public void whenDefiningRuleWithoutThen_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("then")));
        tx.execute(Graql.define(label("a-rule").sub(label(RULE.getLabel())).when(var("x").isa("movie"))));
    }

    @Test
    public void whenDefiningANonRuleWithAWhenPattern_Throw() {
        Statement rule = label("yes").sub(label(ENTITY.getLabel())).when(var("x"));

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
        Statement rule = label("covfefe").sub(label(ENTITY.getLabel())).then(var("x"));

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
        exception.expectMessage(GraqlQueryException.defineUnsupportedProperty(IsaProperty.NAME).getMessage());

        tx.execute(Graql.define(var("x").isa("movie")));
    }

    @Test
    public void whenModifyingAThingInADefineQuery_Throw() {
        ConceptId id = tx.getEntityType("movie").instances().iterator().next().id();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                is(GraqlQueryException.defineUnsupportedProperty(HasAttributeProperty.NAME).getMessage()),
                is(GraqlQueryException.defineUnsupportedProperty(ValueProperty.NAME).getMessage())
        ));

        tx.execute(Graql.define(var().id(id).has("title", "Bob")));
    }

    @Test
    public void whenSpecifyingLabelOfAnExistingConcept_LabelIsChanged() {
        tx.putEntityType("a-new-type");

        EntityType type = tx.getEntityType("a-new-type");
        Label newLabel = Label.of("a-new-new-type");

        tx.execute(Graql.define(label(newLabel).id(type.id())));

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
        tx.execute(Graql.define(label("marriage").sub(label(RELATIONSHIP.getLabel())).relates("husband").relates("wife")));

        RelationshipType marriage = tx.getRelationshipType("marriage");
        Role husband = tx.getRole("husband");
        Role wife = tx.getRole("wife");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(husband, wife));
    }

    @Test
    public void whenDefiningARelationship_SubRoleCasUseAs() {
        tx.execute(Graql.define(label("parentship").sub(label(RELATIONSHIP.getLabel()))
                          .relates("parent")
                          .relates("child")));
        tx.execute(Graql.define(label("fatherhood").sub(label(RELATIONSHIP.getLabel()))
                          .relates("father", "parent")
                          .relates("son", "child")));

        RelationshipType marriage = tx.getRelationshipType("fatherhood");
        Role father = tx.getRole("father");
        Role son = tx.getRole("son");
        assertThat(marriage.roles().toArray(), arrayContainingInAnyOrder(father, son));
        assertEquals(tx.getRole("parent"), father.sup());
        assertEquals(tx.getRole("child"), son.sup());
    }

    @Test
    public void whenDefiningARule_SubRuleDeclarationsCanBeSkipped() {
        Pattern when = Pattern.parse("$x isa entity");
        Pattern then = Pattern.parse("$x isa entity");
        Statement vars = label("my-rule").when(when).then(then);
        tx.execute(Graql.define(vars));

        assertNotNull(tx.getRule("my-rule"));
    }

    @Test
    public void whenDefiningARelationship_SubRoleDeclarationsCanBeSkipped_EvenWhenRoleInReferredToInOtherContexts() {
        tx.execute(Graql.define(
                label("marriage").sub(label(RELATIONSHIP.getLabel())).relates("husband").relates("wife"),
                label("person").plays("husband").plays("wife")
        ));

        RelationshipType marriage = tx.getRelationshipType("marriage");
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
                label("marriage").sub(label(RELATIONSHIP.getLabel())).relates("husband").relates("wife"),
                label("wife").sub(label(ENTITY.getLabel()))
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
