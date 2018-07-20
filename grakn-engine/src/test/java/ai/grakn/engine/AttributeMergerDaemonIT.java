package ai.grakn.engine;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.util.SimpleURI;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class AttributeMergerDaemonIT {
    // Unique Attribute
    // - merge
    // - optimise with janus unique index
    // - reduce by propertyUnique (deprecate)

    @Test
    public void attributeMergeShouldWorkWithin3Seconds() throws InterruptedException {
        SimpleURI grakn = new SimpleURI("localhost", 48555);
        String keyspaceFriendlyNameWithDate = ("grakn_" + (new Date()).toString().replace(" ", "_").replace(":", "_")).toLowerCase();
        Keyspace keyspace = Keyspace.of(keyspaceFriendlyNameWithDate); //
        System.out.println("testing performed on keyspace '" + keyspaceFriendlyNameWithDate + "'");

        // turn off janus index and propertyUnique

        // insert 2 "John"
        try (Grakn.Session session = Grakn.session(grakn, keyspace)) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                AttributeType name = tx.putAttributeType("name", AttributeType.DataType.STRING);
                EntityType person = tx.putEntityType("person").has(name);

                Entity person1 = person.create();
                Entity person2 = person.create();
                Attribute name1 = name.create("John");
                Attribute name2 = name.create("John");
                System.out.println("person: " + person1.id() + ", " + person2.id());
                System.out.println("name: " + name1.id() + ", " + name2.id());
                person1.has(name1);
                person2.has(name2);

                tx.commit();
            }
        }

        System.out.println("Thread.sleep(3000) in order to wait until the merging process finished...");
        Thread.sleep(20000); // wait for the merging operation to complete
        System.out.println("finished waiting.");

        // TODO: enable
        try (Grakn.Session session = Grakn.session(grakn, keyspace)) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
                AttributeType<String> name = tx.getAttributeType("name");
                EntityType person = tx.getEntityType("person");

                List<Entity> people = person.instances().collect(Collectors.toList());
                Set<ConceptId> names = name.instances().map(e -> e.id()).collect(Collectors.toSet());
                Set<ConceptId> namesAssociatedToPeople = people.stream().flatMap(p -> p.attributes(name)).map(e -> e.id()).collect(Collectors.toSet());

                // assert that there is only one real "John" and the duplicate is deleted
                assertThat(names.size(), equalTo(1));

                // assert that every entity connected to the duplicate "John" is connected to the real "John"
                assertThat(names, equalTo(namesAssociatedToPeople));
            }
        }
    }
}
