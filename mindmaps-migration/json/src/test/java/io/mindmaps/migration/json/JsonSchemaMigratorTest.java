/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.migration.json;

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.concept.ResourceType.DataType;
import io.mindmaps.graql.internal.util.GraqlType;
import mjson.Json;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

public class JsonSchemaMigratorTest {

    private MindmapsGraph graph;
    private JsonSchemaMigrator migrator;

    @org.junit.Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        graph =  Mindmaps.factory(Mindmaps.IN_MEMORY).getGraph("default");
        migrator = new JsonSchemaMigrator().graph(graph);
    }

    @After
    public void shutdown() {
        graph.clear();
    }

    @Test
    public void testEmptySchema() {
        Json.Schema schema = Util.readSchema("empty-schema");
        migrator.migrateSchema("thing", schema);
        assertNotNull(graph.getType("thing"));
    }

    @Test
    public void testMigrateSimpleSchema() throws URISyntaxException {
        Json.Schema schema = Util.readSchema("simple-schema");

        assertNull(graph.getType("person"));

        Type person = migrator.migrateSchema(schema);

        assertEquals(graph.getType("person"), person);
        assertNull(graph.getRelationType("has-person"));

        Type address = graph.getType("address");
        assertRelationExists(address, person);
        assertNotNull(address);

        ResourceType streetAddress = graph.getResourceType("streetAddress");
        assertNotNull(streetAddress);
        assertResourceRelationExists(streetAddress, address);
        assertEquals(DataType.STRING, streetAddress.getDataType());

        ResourceType city = graph.getResourceType("city");
        assertNotNull(city);
        assertResourceRelationExists(city, address);
        assertEquals(DataType.STRING, city.getDataType());

        Type phoneNumber = graph.getType("phoneNumber");
        assertNotNull(phoneNumber);
        assertRelationExists(phoneNumber, person);

        Type phoneNumberItem = graph.getType("phoneNumber-item");
        assertNotNull(phoneNumberItem);
        assertRelationExists(phoneNumberItem, phoneNumber);

        ResourceType location = graph.getResourceType("location");
        assertNotNull(location);
        assertResourceRelationExists(location, phoneNumberItem);
        assertEquals(DataType.STRING, location.getDataType());

        ResourceType code = graph.getResourceType("code");
        assertNotNull(code);
        assertResourceRelationExists(code, phoneNumberItem);
        assertEquals(DataType.LONG, code.getDataType());
    }

    @Test
    public void testAllTypes() {
        Json.Schema schema = Util.readSchema("all-types");

        migrator.migrateSchema(schema);

        assertNotNull(graph.getType("an-object"));
        assertNotNull(graph.getType("array-of-ints"));
        assertResourceExists("array-of-ints-item",  DataType.LONG);
        assertResourceExists("a-boolean",  DataType.BOOLEAN);
        assertResourceExists("a-number",  DataType.DOUBLE);
        assertResourceExists("a-string", DataType.STRING);
        assertNotNull(graph.getType("a-null"));
    }

    @Test
    public void testMultipleResources() {
        Json.Schema schema = Util.readSchema("multiple-resources");

        migrator.migrateSchema(schema);

        ResourceType multi = graph.getResourceType("multi");
        assertNotNull(multi);
        // should be assertNull, but resources must have non-null datatypes
        assertEquals(DataType.STRING, multi.getDataType());

        ResourceType multiStr = graph.getResourceType("multi-string");
        assertNotNull(multiStr);
        assertEquals(DataType.STRING, multiStr.getDataType());
        assertEquals(multi, multiStr.superType());

        ResourceType multiInt = graph.getResourceType("multi-integer");
        assertNotNull(multiInt);
        assertEquals(DataType.LONG, multiInt.getDataType());
        assertEquals(multi, multiInt.superType());
    }

    @Test
    public void testArrayOrObject() {
        Json.Schema schema = Util.readSchema("array-or-object");

        migrator.migrateSchema(schema);

        Type thing = graph.getType("thing");
        assertEquals(graph.getMetaEntityType(), thing.type());

        Type thingObj = graph.getType("thing-object");
        assertEquals(thing, thingObj.superType());
        Type thingArr = graph.getType("thing-array");
        assertEquals(thing, thingArr.superType());
    }

    @Test
    public void testStringOrObject() {
        Json.Schema schema = Util.readSchema("string-or-object");

        migrator.migrateSchema(schema);

        Type outerThing = graph.getType("outer-thing");
        assertNotNull(outerThing);
        assertEquals(1, outerThing.playsRoles().size());

        Type thing = graph.getType("the-thing");
        assertNotNull(thing);

        Type thingObj = graph.getResourceType("the-thing-object");
        assertEquals(thing, thingObj.superType());
        assertEquals(1, thingObj.playsRoles().size());

        Type thingStr = graph.getResourceType("the-thing-string");
        assertEquals(thing, thingStr.superType());
        assertEquals(0, thingStr.playsRoles().size());
    }

    @Test
    public void testObjectOrArray() {
        Json.Schema schema = Util.readSchema("object-or-array");

        migrator.migrateSchema(schema);

        Type thing = graph.getType("thing");
        assertNotNull(thing);

        Type thingObj = graph.getType("thing-object");
        assertEquals(thing, thingObj.superType());
        assertEquals(1, thingObj.playsRoles().size());

        Type thingArr = graph.getType("thing-array");
        assertEquals(thing, thingArr.superType());

        Type thingArrItems = graph.getResourceType("thing-array-item");
        assertResourceRelationExists(thingArrItems, thingArr);
    }

    @Test
    public void testAlephEntity() {
        Json.Schema schema = Util.readSchema("aleph-entity");
        Type entity = migrator.migrateSchema(schema);
        assertEquals(graph.getType("entity-entity"), entity);

        assertNullableStringResourceExists(entity, "id");
        assertNullableStringResourceExists(entity, "name");
        assertNullableStringResourceExists(entity, "summary");
        assertNullableStringResourceExists(entity, "description");
        assertNullableStringResourceExists(entity, "jurisdiction_code");
        assertNullableStringResourceExists(entity, "register_name");

        ResourceType state = graph.getResourceType("state");
        assertResourceRelationExists(state, entity);
        assertEquals(DataType.STRING, state.getDataType());
        assertEquals("pending|active|deleted", state.getRegex());

        ResourceType registerUrl = graph.getResourceType("register_url");
        assertResourceRelationExists(registerUrl, entity);
        assertEquals(DataType.STRING, registerUrl.getDataType());
    }

    @Test
    public void testAlephAlert() {
        Type alert = migrateAleph("alert");
        assertEquals("alert", alert.getId());
    }

    @Test
    public void testAlephCollection() {
        Type collection = migrateAleph("collection");
        assertEquals("collection", collection.getId());

        ResourceType label = graph.getResourceType("label");
        assertEquals(".{2,255}", label.getRegex());
    }

    @Test
    public void testAlephPermission() {
        Type permission = migrateAleph("permission");
        assertEquals("permission", permission.getId());
    }

    @Test
    @Ignore
    public void testAlephRole() {
        Type role = migrateAleph("role");
        assertEquals("role", role.getId());

        // Email regex is crazy
        ResourceType email = graph.getResourceType("email-string");
        assertEquals(
                "([!#-'*+/-9=?A-Z^-~-]+(\\.[!#-'*+/-9=?A-Z^-~-]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@"+
                        "([!#-'*+/-9=?A-Z^-~-]+(\\.[!#-'*+/-9=?A-Z^-~-]+)*|\\[[\t -Z^-~]*])", email.getRegex()
        );
    }

    @Test
    public void testAlephSource() {
        Type source = migrateAleph("source");
        assertEquals("source", source.getId());
    }

    @Test
    public void testAlephAddress() {
        Type address = migrateAleph("entity/address");
        assertEquals("entity-address", address.getId());
    }

    @Test
    public void testAlephAsset() {
        Type asset = migrateAleph("entity/asset");
        assertEquals("entity-asset", asset.getId());

        // Make sure $ref has worked correctly
        Type ownersItems = graph.getType("owners-item");

        assertRelationExists(graph.getType("owner"), ownersItems);
    }

    @Test
    public void testBadType() {
        Json.Schema schema = Util.readSchema("bad-type");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("not-a-type");

        migrator.migrateSchema("thing", schema);
    }

    private void assertRelationExists(Type other, Type owner) {
        RelationType relationType = graph.getRelationType("has-" + other.getId());
        RoleType roleOwner = graph.getRoleType(other.getId() + "-owner");
        RoleType roleOther = graph.getRoleType(other.getId() + "-role");

        assertNotNull(relationType);
        assertNotNull(roleOwner);
        assertNotNull(roleOther);

        assertEquals(relationType, roleOwner.relationType());
        assertEquals(relationType, roleOther.relationType());

        assertTrue(owner.playsRoles().contains(roleOwner));
        assertTrue(other.playsRoles().contains(roleOther));
    }

    private void assertResourceRelationExists(Type other, Type owner) {
        RelationType relationType = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(other.getId()));
        RoleType roleOwner = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(other.getId()));
        RoleType roleOther = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(other.getId()));

        assertNotNull(relationType);
        assertNotNull(roleOwner);
        assertNotNull(roleOther);

        assertEquals(relationType, roleOwner.relationType());
        assertEquals(relationType, roleOther.relationType());

        assertTrue(owner.playsRoles().contains(roleOwner));
        assertTrue(other.playsRoles().contains(roleOther));
    }

    private void assertResourceExists(String name, DataType datatype) {
        ResourceType resourceType = graph.getResourceType(name);
        assertNotNull(resourceType);
        assertEquals(datatype, resourceType.getDataType());
    }

    private void assertNullableStringResourceExists(Type owner, String name) {
        ResourceType resourceType = graph.getResourceType(name);
        // should be assertNull, but resources must have non-null datatypes
        assertEquals(DataType.STRING, resourceType.getDataType());

        assertResourceRelationExists(resourceType, owner);

        ResourceType resourceString = graph.getResourceType(name + "-string");
        assertEquals(resourceType, resourceString.superType());
        assertEquals(DataType.STRING, resourceString.getDataType());

        ResourceType resourceNull = graph.getResourceType(name + "-null");
        assertEquals(resourceType, resourceNull.superType());
        // should be assertNull, but resources must have non-null datatypes
        assertEquals(DataType.STRING, resourceNull.getDataType());
    }

    private Type migrateAleph(String name) {
        URL alephBase = getClass().getClassLoader().getResource("aleph");
        URL alephSchema = getClass().getClassLoader().getResource("aleph/" + name + ".json");
        assert alephBase != null;
        assert alephSchema != null;
        URI alephBaseUri;
        URI alephSchemaUri;
        try {
            alephBaseUri = alephBase.toURI();
            alephSchemaUri = alephSchema.toURI();
        } catch (URISyntaxException e) {
            fail();
            return null;
        }

        Json.Function<URI, Json> relativeReferenceResolver = docuri -> {
            try {
                return Json.read(new File(alephBaseUri.getPath() + docuri.getPath()).toURI().toURL());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };

        Json.Schema schema = Json.schema(alephSchemaUri, relativeReferenceResolver);
        return migrator.migrateSchema(schema);
    }
}
