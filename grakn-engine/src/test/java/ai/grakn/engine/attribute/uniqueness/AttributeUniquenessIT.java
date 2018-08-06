package ai.grakn.engine.attribute.uniqueness;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.util.SimpleURI;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

public class AttributeUniquenessIT {
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

        // TODO: check that we've turned off janus index and propertyUnique

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

    @Test
    public void shouldBeAbleToMergeManyAttributesIntoOne() {
        String grakn = "localhost:48555";
        String merge = "http://localhost:4567/merge";
        String keyspaceFriendlyNameWithDate = ("grakn_" + (new Date()).toString().replace(" ", "_").replace(":", "_")).toLowerCase();
        Keyspace keyspace = Keyspace.of(keyspaceFriendlyNameWithDate);
        String name = "John";
        int duplicateCount = 180;

        // TODO: check that we've turned off janus index and propertyUnique

        try (Grakn.Session session = Grakn.session(new SimpleURI(grakn), keyspace)) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                tx.graql().define(
                        label("name").sub("attribute").datatype(AttributeType.DataType.STRING),
                        label("parent").sub("role"),
                        label("child").sub("role"),
                        label("person").sub("entity").has("name").plays("parent").plays("child"),
                        label("parentchild").sub("relationship").relates("parent").relates("child")
                ).execute();
                tx.commit();
            }
        }

        try (Grakn.Session session = Grakn.session(new SimpleURI(grakn), keyspace)) {
            System.out.println("inserting a new name attribute with value '" + name + "'...");
            for (int i = 0; i < duplicateCount; ++i) {
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    tx.graql().insert(var().isa("name").val(name)).execute();
                    tx.commit();
                }
            }
            System.out.println("done.");
        }

        // merge
        int mergeIter = 0;
        while (true) {
            System.out.println("triggering merge.");
            triggerMerge(merge);
            System.out.println("merge completed");
            if (mergeIter >= duplicateCount) {
                System.out.println("we've triggered the merge operation " + mergeIter + " times to merge " + duplicateCount + " duplicates. Soo... HOPEFULLY... there's no more duplicates exist. merge finished");
                break;
            }
            mergeIter++;
        }

        try (Grakn.Session session = Grakn.session(new SimpleURI(grakn), keyspace)) {
            System.out.println("inserting a new name attribute with value '" + name + "'...");
            for (int i = 0; i < duplicateCount; ++i) {
                try (Grakn.Transaction tx = session.transaction(GraknTxType.WRITE)) {
                    tx.graql().insert(var().isa("name").val(name)).execute();
                    tx.commit();
                }
            }
            System.out.println("done.");
        }

        // merge
        int mergeIter2 = 0;
        while (true) {
            System.out.println("triggering merge.");
            int remainingAttributeInQueue = triggerMerge(merge);
            System.out.println(remainingAttributeInQueue + " attributes removed");
            if (mergeIter2 >= duplicateCount) {
                System.out.println("we've triggered the merge operation " + mergeIter + " times to merge " + duplicateCount + " duplicates. Soo... HOPEFULLY... there's no more duplicates exist. merge finished");
                break;
            }
            mergeIter2++;
        }

        try (Grakn.Session session = Grakn.session(new SimpleURI(grakn), keyspace)) {
            try (Grakn.Transaction tx = session.transaction(GraknTxType.READ)) {
                List<Concept> concept = tx.graql().match(var("n").isa("name")).get().execute()
                        .stream().map(e -> e.get(var("n"))).collect(Collectors.toList());
                assertThat(concept, hasSize(1));
            }
        }
    }

    /**
     * Attempts to trigger an attribute merge operation by sending a GET request to the supplied URL.
     * Returns back the count of items still in the Queue queue after merge
     */
    private int triggerMerge(String url) {
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            if (con.getResponseCode() != 200) {
                throw new RuntimeException("request to '" + url + "', returned with HTTP status code" + con.getResponseCode());
            }
            try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }

                return Integer.parseInt(response.toString());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
