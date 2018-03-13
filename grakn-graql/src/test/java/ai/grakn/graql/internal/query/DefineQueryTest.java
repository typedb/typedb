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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.DefineQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.pattern.property.HasAttributeProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.concept.AttributeType.DataType.BOOLEAN;
import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.parse;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlTestUtil.assertExists;
import static ai.grakn.util.GraqlTestUtil.assertNotExists;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static ai.grakn.util.Schema.ImplicitType.HAS_OWNER;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;
import static ai.grakn.util.Schema.ImplicitType.KEY;
import static ai.grakn.util.Schema.ImplicitType.KEY_OWNER;
import static ai.grakn.util.Schema.ImplicitType.KEY_VALUE;
import static ai.grakn.util.Schema.MetaSchema.ENTITY;
import static ai.grakn.util.Schema.MetaSchema.RELATIONSHIP;
import static ai.grakn.util.Schema.MetaSchema.ROLE;
import static ai.grakn.util.Schema.MetaSchema.RULE;
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
public class DefineQueryTest {

    private QueryBuilder qb;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final SampleKBContext movies = MovieKB.context();

    @Before
    public void setUp() {
        qb = movies.tx().graql();
    }

    @After
    public void clear(){
        movies.rollback();
    }

    @Test
    public void testDefineSub() {
        assertDefine(var("x").label("cool-movie").sub("movie"));
    }

    @Test
    public void testDefineSchema() {
        qb.define(
                label("pokemon").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("evolution").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()),
                label("evolves-from").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolves-to").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("evolution").relates("evolves-from").relates("evolves-to"),
                label("pokemon").plays("evolves-from").plays("evolves-to").has("name")
        ).execute();

        assertExists(qb, label("pokemon").sub(ENTITY.getLabel().getValue()));
        assertExists(qb, label("evolution").sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(qb, label("evolves-from").sub(ROLE.getLabel().getValue()));
        assertExists(qb, label("evolves-to").sub(ROLE.getLabel().getValue()));
        assertExists(qb, label("evolution").relates("evolves-from").relates("evolves-to"));
        assertExists(qb, label("pokemon").plays("evolves-from").plays("evolves-to"));
    }

    @Test
    public void testDefineIsAbstract() {
        qb.define(
                label("concrete-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("abstract-type").isAbstract().sub(Schema.MetaSchema.ENTITY.getLabel().getValue())
        ).execute();

        assertNotExists(qb, label("concrete-type").isAbstract());
        assertExists(qb, label("abstract-type").isAbstract());
    }

    @Test
    public void testDefineDataType() {
        qb.define(
                label("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.LONG)
        ).execute();

        Match match = qb.match(var("x").label("my-type"));
        AttributeType.DataType datatype = match.iterator().next().get("x").asAttributeType().getDataType();

        Assert.assertEquals(AttributeType.DataType.LONG, datatype);
    }

    @Test
    public void testDefineSubResourceType() {
        qb.define(
                label("my-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                label("sub-type").sub("my-type")
        ).execute();

        Match match = qb.match(var("x").label("sub-type"));
        AttributeType.DataType datatype = match.iterator().next().get("x").asAttributeType().getDataType();

        Assert.assertEquals(AttributeType.DataType.STRING, datatype);
    }

    @Test
    public void testDefineSubRole() {
        qb.define(
                label("marriage").sub(Schema.MetaSchema.RELATIONSHIP.getLabel().getValue()).relates("spouse1").relates("spouse2"),
                label("spouse").sub(Schema.MetaSchema.ROLE.getLabel().getValue()),
                label("spouse1").sub("spouse"),
                label("spouse2").sub("spouse")
        ).execute();

        assertExists(qb, label("spouse1"));
    }

    @Test
    public void testReferenceByVariableNameAndTypeLabel() {
        qb.define(
                var("abc").sub("entity"),
                var("abc").label("123"),
                label("123").plays("actor"),
                var("abc").plays("director")
        ).execute();

        assertExists(qb, label("123").sub("entity"));
        assertExists(qb, label("123").plays("actor"));
        assertExists(qb, label("123").plays("director"));
    }

    @Test
    public void testDefineReferenceByName() {
        String roleTypeLabel = HAS_OWNER.getLabel("title").getValue();
        qb.define(
                label("new-type").sub(Schema.MetaSchema.ENTITY.getLabel().getValue()),
                label("new-type").plays(roleTypeLabel)
        ).execute();

        qb.insert(var("x").isa("new-type")).execute();

        Match typeQuery = qb.match(var("n").label("new-type"));

        assertEquals(1, typeQuery.stream().count());

        // We checked count ahead of time
        //noinspection OptionalGetWithoutIsPresent
        EntityType newType = typeQuery.get("n").findFirst().get().asEntityType();

        assertTrue(newType.plays().anyMatch(role -> role.equals(movies.tx().getRole(roleTypeLabel))));

        assertExists(qb, var().isa("new-type"));
    }

    @Test
    public void testHas() {
        String resourceType = "a-new-resource-type";

        qb.define(
                label("a-new-type").sub("entity").has(resourceType),
                label(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING),
                label("an-unconnected-resource-type").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.LONG)
        ).execute();

        // Make sure a-new-type can have the given resource type, but not other resource types
        assertExists(qb, label("a-new-type").sub("entity").has(resourceType));
        assertNotExists(qb, label("a-new-type").has("title"));
        assertNotExists(qb, label("movie").has(resourceType));
        assertNotExists(qb, label("a-new-type").has("an-unconnected-resource-type"));

        VarPattern hasResource = Graql.label(HAS.getLabel(resourceType));
        VarPattern hasResourceOwner = Graql.label(HAS_OWNER.getLabel(resourceType));
        VarPattern hasResourceValue = Graql.label(HAS_VALUE.getLabel(resourceType));

        // Make sure the expected ontology elements are created
        assertExists(qb, hasResource.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(qb, hasResourceOwner.sub(ROLE.getLabel().getValue()));
        assertExists(qb, hasResourceValue.sub(ROLE.getLabel().getValue()));
        assertExists(qb, hasResource.relates(hasResourceOwner));
        assertExists(qb, hasResource.relates(hasResourceValue));
        assertExists(qb, label("a-new-type").plays(hasResourceOwner));
        assertExists(qb, label(resourceType).plays(hasResourceValue));
    }

    @Test
    public void testKey() {
        String resourceType = "a-new-resource-type";

        qb.define(
                label("a-new-type").sub("entity").key(resourceType),
                label(resourceType).sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING)
        ).execute();

        // Make sure a-new-type can have the given resource type as a key or otherwise
        assertExists(qb, label("a-new-type").sub("entity").key(resourceType));
        assertExists(qb, label("a-new-type").sub("entity").has(resourceType));
        assertNotExists(qb, label("a-new-type").sub("entity").key("title"));
        assertNotExists(qb, label("movie").sub("entity").key(resourceType));

        VarPattern key = Graql.label(KEY.getLabel(resourceType));
        VarPattern keyOwner = Graql.label(KEY_OWNER.getLabel(resourceType));
        VarPattern keyValue = Graql.label(KEY_VALUE.getLabel(resourceType));

        // Make sure the expected ontology elements are created
        assertExists(qb, key.sub(RELATIONSHIP.getLabel().getValue()));
        assertExists(qb, keyOwner.sub(ROLE.getLabel().getValue()));
        assertExists(qb, keyValue.sub(ROLE.getLabel().getValue()));
        assertExists(qb, key.relates(keyOwner));
        assertExists(qb, key.relates(keyValue));
        assertExists(qb, label("a-new-type").plays(keyOwner));
        assertExists(qb, label(resourceType).plays(keyValue));
    }

    @Test
    public void testResourceTypeRegex() {
        qb.define(label("greeting").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue()).datatype(AttributeType.DataType.STRING).regex("hello|good day")).execute();

        Match match = qb.match(var("x").label("greeting"));
        assertEquals("hello|good day", match.get("x").findFirst().get().asAttributeType().getRegex());
    }

    @Test
    public void whenExecutingADefineQuery_ResultContainsAllInsertedVars() {
        Var type = var("type");
        Var type2 = var("type2");

        // Note that two variables refer to the same type. They should both be in the result
        DefineQuery query = qb.define(type.label("my-type").sub("entity"), type2.label("my-type"));

        Answer result = query.execute();
        assertThat(result.vars(), containsInAnyOrder(type, type2));
        assertEquals(result.get(type), result.get(type2));
    }

    @Test
    public void whenChangingTheSuperOfAnExistingConcept_ApplyTheChange() {
        EntityType newType = movies.tx().putEntityType("a-new-type");
        EntityType movie = movies.tx().getEntityType("movie");

        qb.define(label("a-new-type").sub("movie")).execute();
        
        assertEquals(movie, newType.sup());
    }

    @Test
    public void whenDefiningARule_TheRuleIsInTheKB() {
        Pattern when = qb.parser().parsePattern("$x isa entity");
        Pattern then = qb.parser().parsePattern("$x isa entity");
        VarPattern vars = label("my-rule").sub(label(RULE.getLabel())).when(when).then(then);
        qb.define(vars).execute();

        assertNotNull(movies.tx().getRule("my-rule"));
    }

    @Test
    public void testErrorResourceTypeWithoutDataType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("my-resource"), containsString("datatype"), containsString("resource"))
        );
        qb.define(label("my-resource").sub(Schema.MetaSchema.ATTRIBUTE.getLabel().getValue())).execute();
    }

    @Test
    public void testErrorRecursiveType() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("thingy"), containsString("itself")));
        qb.define(label("thingy").sub("thingy")).execute();
    }

    @Test
    public void whenDefiningAnOntologyConceptWithoutALabel_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("entity"), containsString("label")));
        qb.define(var().sub("entity")).execute();
    }

    @Test
    public void testErrorWhenNonExistentResource() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage("nothing");
        qb.define(label("blah this").sub("entity").has("nothing")).execute();
    }

    @Test
    public void whenDefiningMetaType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(ErrorMessage.INSERT_METATYPE.getMessage("my-metatype", Schema.MetaSchema.THING.getLabel().getValue()));
        qb.define(label("my-metatype").sub(Schema.MetaSchema.THING.getLabel().getValue())).execute();
    }

    @Test
    public void whenSpecifyingExistingTypeWithIncorrectDataType_Throw() {
        AttributeType name = movies.tx().getAttributeType("name");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                GraqlQueryException.insertPropertyOnExistingConcept("datatype", BOOLEAN, name).getMessage()
        );

        movies.tx().graql().define(label("name").datatype(BOOLEAN)).execute();
    }

    @Test
    public void whenSpecifyingDataTypeOnAnEntityType_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(
                allOf(containsString("unexpected property"), containsString("datatype"), containsString("my-type"))
        );

        movies.tx().graql().define(label("my-type").sub("entity").datatype(BOOLEAN)).execute();
    }

    @Test
    public void whenDefiningRuleWithoutWhen_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("when")));
        qb.define(label("a-rule").sub(label(RULE.getLabel())).then(var("x").isa("movie"))).execute();
    }

    @Test
    public void whenDefiningRuleWithoutThen_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(allOf(containsString("rule"), containsString("movie"), containsString("then")));
        qb.define(label("a-rule").sub(label(RULE.getLabel())).when(var("x").isa("movie"))).execute();
    }

    @Test
    public void whenDefiningANonRuleWithAWhenPattern_Throw() {
        VarPattern rule = label("yes").sub(label(ENTITY.getLabel())).when(var("x"));

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                // Either we see "entity" and an unexpected "when"...
                allOf(containsString("unexpected property"), containsString("when")),
                // ...or we see "when" and don't find the expected "then"
                containsString(GraqlQueryException.insertNoExpectedProperty("then", rule.admin()).getMessage()))
        );

        qb.define(rule).execute();
    }

    @Test
    public void whenDefiningANonRuleWithAThenPattern_Throw() {
        VarPattern rule = label("covfefe").sub(label(ENTITY.getLabel())).then(var("x"));

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                // Either we see "entity" and an unexpected "when"...
                allOf(containsString("unexpected property"), containsString("then")),
                // ...or we see "when" and don't find the expected "then"
                containsString(GraqlQueryException.insertNoExpectedProperty("when", rule.admin()).getMessage()))
        );

        qb.define(rule).execute();
    }

    @Test
    public void whenDefiningAThing_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.defineUnsupportedProperty(IsaProperty.NAME).getMessage());

        qb.define(var("x").isa("movie")).execute();
    }

    @Test
    public void whenModifyingAThingInADefineQuery_Throw() {
        ConceptId id = movies.tx().getEntityType("movie").instances().iterator().next().getId();

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(anyOf(
                is(GraqlQueryException.defineUnsupportedProperty(HasAttributeProperty.NAME).getMessage()),
                is(GraqlQueryException.defineUnsupportedProperty(ValueProperty.NAME).getMessage())
        ));

        qb.define(var().id(id).has("title", "Bob")).execute();
    }

    @Test
    public void whenSpecifyingLabelOfAnExistingConcept_LabelIsChanged() {
        movies.tx().putEntityType("a-new-type");

        EntityType type = movies.tx().getEntityType("a-new-type");
        Label newLabel = Label.of("a-new-new-type");

        qb.define(label(newLabel).id(type.getId())).execute();

        assertEquals(newLabel, type.getLabel());
    }

    @Test
    public void whenCallingToStringOfDefineQuery_ReturnCorrectRepresentation(){
        String queryString = "define label my-entity sub entity;";
        DefineQuery defineQuery = parse(queryString);
        assertEquals(queryString, defineQuery.toString());
    }

    @Test
    public void whenDefiningARelationship_SubRoleDeclarationsCanBeSkipped() {
        qb.define(label("marriage").sub(label(RELATIONSHIP.getLabel())).relates("husband").relates("wife")).execute();

        RelationshipType marriage = movies.tx().getRelationshipType("marriage");
        Role husband = movies.tx().getRole("husband");
        Role wife = movies.tx().getRole("wife");
        assertThat(marriage.relates().toArray(), arrayContainingInAnyOrder(husband, wife));
    }

    @Test
    public void whenDefiningARule_SubRuleDeclarationsCanBeSkipped() {
        Pattern when = qb.parser().parsePattern("$x isa entity");
        Pattern then = qb.parser().parsePattern("$x isa entity");
        VarPattern vars = label("my-rule").when(when).then(then);
        qb.define(vars).execute();

        assertNotNull(movies.tx().getRule("my-rule"));
    }

    @Test
    public void whenDefiningARelationship_SubRoleDeclarationsCanBeSkipped_EvenWhenRoleInReferredToInOtherContexts() {
        qb.define(
                label("marriage").sub(label(RELATIONSHIP.getLabel())).relates("husband").relates("wife"),
                label("person").plays("husband").plays("wife")
        ).execute();

        RelationshipType marriage = movies.tx().getRelationshipType("marriage");
        EntityType person = movies.tx().getEntityType("person");
        Role husband = movies.tx().getRole("husband");
        Role wife = movies.tx().getRole("wife");
        assertThat(marriage.relates().toArray(), arrayContainingInAnyOrder(husband, wife));
        assertThat(person.plays().toArray(), hasItemInArray(wife));
        assertThat(person.plays().toArray(), hasItemInArray(husband));
    }

    @Test
    public void whenDefiningARelationshipWithNonRoles_Throw() {
        exception.expect(GraknException.class);

        qb.define(
                label("marriage").sub(label(RELATIONSHIP.getLabel())).relates("husband").relates("wife"),
                label("wife").sub(label(ENTITY.getLabel()))
        ).execute();
    }

    private void assertDefine(VarPattern... vars) {
        // Make sure vars don't exist
        for (VarPattern var : vars) {
            assertNotExists(qb, var);
        }

        // Define all vars
        qb.define(vars).execute();

        // Make sure all vars exist
        for (VarPattern var : vars) {
            assertExists(qb, var);
        }

        // Undefine all vars
        qb.undefine(vars).execute();

        // Make sure vars don't exist
        for (VarPattern var : vars) {
            assertNotExists(qb, var);
        }
    }
}
