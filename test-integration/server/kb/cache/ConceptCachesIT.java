package grakn.core.server.kb.cache;

import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.AttributeType;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.session.SessionImpl;
import grakn.core.server.session.TransactionOLTP;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class ConceptCachesIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction().write();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }

    @Test
    public void whenCreatingResource_EnsureTheResourcesDataTypeIsTheSameAsItsType() throws Exception {
        AttributeType<String> attributeType = tx.putAttributeType("attributeType", AttributeType.DataType.STRING);
        Attribute attribute = attributeType.create("resource");
        assertEquals(AttributeType.DataType.STRING, attribute.dataType());
    }
}
