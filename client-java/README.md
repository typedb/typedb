# Grakn Client Java

## Dependencies
The only dependency for getting started with Grakn Client Java is `Grakn >= 1.3.0` added as a dependency in your Java project.

### Grakn Core

```xml
<dependency>
  <groupId>ai.grakn</groupId>
  <artifactId>client-java</artifactId>
  <version>1.4.3</version>
</dependency>
```

### Grakn KGMS

```xml
<dependency>
  <groupId>ai.grakn.kgms</groupId>
  <artifactId>client</artifactId>
  <version>1.4.3</version>
</dependency>
```

## Quickstart
First make sure, the [Grakn server](http://dev.grakn.ai/docs/running-grakn/install-and-run#start-the-grakn-server) is running.

In the interpreter or in your source, import `grakn`.

Instantiate a client and open a session.

```java
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.util.SimpleURI;

public class GraknQuickstart {
    public static void main(String[] args) {
        SimpleURI localGrakn = new SimpleURI("localhost", 48555);
        Keyspace keyspace = Keyspace.of("genealogy");
        Grakn grakn = new Grakn(localGrakn);
        Grakn.Session session = grakn.session(keyspace);
        // session is open
        session.close();
        // session is closed
    }
}
```

We can also pass the credentials, as specified when [configuring authentication via Grakn Console](http://dev.grakn.ai/docs/management/users).

```java
SimpleURI localGrakn = new SimpleURI("localhost", 48555);
Grakn grakn = new ClientFactory(localGrakn, "<username>", "<password>").client();
```

Create transactions to use for reading and writing data.

```java
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.util.SimpleURI;

public class GraknQuickstart {
    public static void main(String[] args) {
        SimpleURI localGrakn = new SimpleURI("localhost", 48555);
        Keyspace keyspace = Keyspace.of("genealogy");
        Grakn grakn = new Grakn(localGrakn);
        Grakn.Session session = grakn.session(keyspace);

        // creating a write transaction
        Grakn.Transaction writeTransaction = session.transaction(GraknTxType.WRITE);
        // write transaction is open
        // write transaction must always be committed (closed)
        writeTransaction.commit();

        // creating a read transaction
        Grakn.Transaction readTransaction = session.transaction(GraknTxType.READ);
        // read transaction is open
        // read transaction must always be closed
        readTransaction.close();

        session.close();
    }
}
```

Running basic retrieval and insertion queries.

```java
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.client.Grakn;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.SimpleURI;
import java.util.List;
import java.util.stream.Stream;

public class GraknQuickstart {
    public static void main(String[] args) {
      SimpleURI localGrakn = new SimpleURI("localhost", 48555);
      Keyspace keyspace = Keyspace.of("phone_calls");
      Grakn grakn = new Grakn(localGrakn);
      Grakn.Session session = grakn.session(keyspace);

      Grakn.Transaction writeTransaction = session.transaction(GraknTxType.WRITE);
      InsertQuery insertQuery = Graql.insert(var("p").isa("person").has("first-name", "Elizabeth"));
      List<ConceptMap> insertedId = insertQuery.withTx(writeTransaction).execute();
      System.out.println("Inserted a person with ID: " + insertedId.get(0).get("p").id());
      writeTransaction.commit();

      Grakn.Transaction readTransaction = session.transaction(GraknTxType.READ);
      GetQuery query = Graql.match(var("p").isa("person")).limit(10).get();
      Stream<ConceptMap> answers = query.withTx(readTransaction).stream();
      answers.forEach(answer -> System.out.println(answer.get("p").id()));
      readTransaction.close();

      session.close();
    }
}
```

**Remember that transactions always need to be closed. Commiting a write transaction closes it. A read transaction, however, must be explicitly clased by calling the `close()` method on it.**

To view examples of running various Graql queries using the Grakn Client Java, head over to their dedicated documentation pages as listed below.

- [Insert](http://dev.grakn.ai/docs/query/insert-query)
- [Get](http://dev.grakn.ai/docs/query/get-query)
- [Delete](http://dev.grakn.ai/docs/query/delete-query)
- [Aggregate](http://dev.grakn.ai/docs/query/aggregate-query)
- [Compute](http://dev.grakn.ai/docs/query/compute-query)

## Client Architecture
To learn about the mechanism that a Grakn Client uses to set up communication with keyspaces running on the Grakn Server, refer to [Grakn > Client API > Overview]((http://dev.grakn.ai/docs/client-api/overview).

## API Reference
To learn about the methods available for executing queries and retrieving their answers using Client Java, refer to [Grakn > Client API > Java > API Reference](http://dev.grakn.ai/docs/client-api/java#api-reference).

## Concept API
To learn about the methods available on the concepts retrieved as the answers to Graql queries, refer to [Grakn > Concept API > Overview](http://dev.grakn.ai/docs/concept-api/overview)