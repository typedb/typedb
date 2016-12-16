package ai.grakn.test.graql.printer;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.test.AbstractMovieGraphTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import mjson.Json;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Optional;

import static mjson.Json.array;
import static mjson.Json.nil;
import static mjson.Json.object;
import static mjson.Json.read;
import static org.junit.Assert.assertEquals;

public class HALPrinterTest extends AbstractMovieGraphTest {

    private Printer printer;

    @Before
    public void setUp() {
        printer = Printers.hal();
    }

    @Test
    public void testJsonEmptyMap() {
        assertJsonEquals(object(), new HashMap<>());
    }

    @Test
    public void testJsonMapStringString() {
        assertJsonEquals(object("key", "value"), ImmutableMap.of("key", "value"));
    }

    @Test
    public void testJsonMapIntInt() {
        assertJsonEquals(object("1", 2), ImmutableMap.of(1, 2));
    }

    @Test
    public void testJsonEmptyList() {
        assertJsonEquals(array(), Lists.newArrayList());
    }

    @Test
    public void testJsonListOptionalEmpty() {
        assertJsonEquals(array(nil()), ImmutableList.of(Optional.empty()));
    }

    @Test
    public void testJsonListOptionalPresent() {
        assertJsonEquals(array(true), ImmutableList.of(Optional.of(true)));
    }

    @Test
    public void testJsonMetaConcept() {
        Type entityType = graph.admin().getMetaConcept();
        Json json = read(printer.graqlString(entityType));
        assertEquals("TYPE", json.at("_baseType").asString());
    }

    @Test
    public void testJsonMetaType() {
        Type entityType = graph.admin().getMetaEntityType();
        Json json = read(printer.graqlString(entityType));
        assertEquals("ENTITY_TYPE", json.at("_baseType").asString());
    }

    @Test
    public void testJsonEntityType() {
        EntityType movie = graph.getEntityType("movie");
        Json json = read(printer.graqlString(movie));
        assertEquals("movie", json.at("_id").asString());
    }

    private void assertJsonEquals(Json expected, Object object) {
        Json json = read(printer.graqlString(object));
        assertEquals(expected, json);
    }
}
