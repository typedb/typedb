# Using Grakn To Work With Tweet Data
In this tutorial we will look at how to stream public tweets into Grakn's knowledge graph. The tutorial aims to demonstrate key concepts such as receiving, inserting and querying data. Upon the completion of this tutorial, you will have learnt about these concepts:

- Defining a simple Grakn.ai ontology using the Java API
- Streaming public tweets into the application with the [Twitter4J](http://twitter4j.org/ "Twitter4J") library
- Inserting tweets into the knowledge graph using Grakn's Graph API
- Performing simple queries using GraQL, the Grakn's Query Language

## Registering Your Own Twitter Application
As of today, you will need a valid credential in order to call practically every endpoint in the Twitter API. Therefore, you must own a Twitter application (or, register a new one) before proceeding further.

You can register your own application in the [Twitter Application Management](https://apps.twitter.com/). Once you've done so you can  get the credentials by visiting the **Keys and Access Tokens** tab. The value we care about in particular are Consumer Key, Consumer Secret, Access Token, and Access Token Secret.

## Bootstraping The Skeleton Project
Let's bootstrap a new maven project! Hit the command line and run the following command to generate a new maven project:

```sh
mvn archetype:generate -DgroupId=ai.grakn -DartifactId=twitterexample -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
```

Now that you have basic project structure and `pom.xml` in place, let's start customising them to our needs. We will add two things to the `<build>` section:

1. `maven-compiler-plugin` configuration in order to enable lambda and other nifty Java 8 features
2. `maven-shade-plugin` configuration which points to our `Main` class, for creating a fat JAR.

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <configuration>
                <source>1.8</source>
                <target>1.8</target>
            </configuration>
        </plugin>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>2.3</version>
            <configuration>
                <createDependencyReducedPom>true</createDependencyReducedPom>
                <filters>
                    <filter>
                        <artifact>*:*</artifact>
                        <excludes>
                            <exclude>META-INF/*.SF</exclude>
                            <exclude>META-INF/*.DSA</exclude>
                            <exclude>META-INF/*.RSA</exclude>
                        </excludes>
                    </filter>
                </filters>
            </configuration>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>ai.grakn.twitterexample.Main</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

Then continue to the `<dependencies>` section and make sure you have all the required dependencies, i.e., `grakn-graph`, `grakn-graql`, `twitter4j-core`, and `twitter4j-stream`:

```xml
<dependencies>
    <dependency>
        <groupId>ai.grakn</groupId>
        <artifactId>grakn-graph</artifactId>
        <version>0.13.0</version>
    </dependency>
    <dependency>
        <groupId>ai.grakn</groupId>
        <artifactId>grakn-graql</artifactId>
        <version>0.13.0</version>
    </dependency>

    <dependency>
        <groupId>org.twitter4j</groupId>
        <artifactId>twitter4j-core</artifactId>
        <version>4.0.6</version>
    </dependency>

    <dependency>
        <groupId>org.twitter4j</groupId>
        <artifactId>twitter4j-stream</artifactId>
        <version>4.0.6</version>
    </dependency>
</dependencies>
```

You can see the complete `pom.xml` definition [here](google.com).

## The Main Class

Let's kick things off by defining a `Main` class inside the `ai.grakn.twitterexample` package. Aside from Twitter credentials, it contains a few important Grakn settings.

First, we have decided to use an **in-memory graph** for simplicity's sake — working with an in-memory graph frees us from having to set up a Grakn distribution in the local machine. The in-memory graph is not for storing data and will be lost once the program finishes execution. Second, the graph will be stored in a **keyspace** named `twitter-example`.

```java
package ai.grakn.twitterexample;

import ai.grakn.Grakn;
import ai.grakn.GraknSession;

public class Main {
  // Twitter credentials
  private static final String consumerKey = "...";
  private static final String consumerSecret = "...";
  private static final String accessToken = "...";
  private static final String accessTokenSecret = "...";

  // Grakn settings
  private static final String graphImplementation = Grakn.IN_MEMORY;
  private static final String keyspace = "twitter-example";

  public static void main(String[] args) {
    // our code will go here
  }
}
```

We then define a `GraknSession` object in `main()`. Enclosing it in a `try-with-resource` construct is a good practice, lest we forget closing the session by calling `session.close()`.

```java
public static void main(String[] args) {
  try (GraknSession session = Grakn.session(graphImplementation, keyspace)) {
    // our code will go here
  }
}
```

Following that, another equally important object for operating on the graph is `GraknGraph`. After performing the operations we desire, we must not forget to commit. For convenience, let's define a helper method which opens a `GraknGraph` in write mode, and commits it after executing the function `fn`. We will be using this function in various places throughout the tutorial.

```java
public class GraknTweetOntologyHelper {
  public static void withGraknGraph(GraknSession session, Consumer<GraknGraph> fn) {
    GraknGraph graphWriter = session.open(GraknTxType.WRITE);
    fn.accept(graphWriter);
    graphWriter.commit();
  }
}
```

We have decided to omit exception handling to keep the tutorial simple. In production code however, it will be very important and must not be forgotten.

## Defining The Ontology

Let's define the ontology. As we are mainly interested in both the **tweet** and **who posted the tweet**, let us capture these concepts by defining two **entities**: `user` and `tweet`.

The `user` entity will hold the user's actual username in a **resource** called `screen_name`, while the `tweet` entity will contain the user's tweet in another resource called `text`. We will also define a resource `identifier` for the id.

Next we will define two **roles** - `posts` and `posted_by` to express that a `user` posts a `tweet`, and similarly, a `tweet` is posted by a `user`. We will tie this two roles by a **relation** called `user-tweet-relation`.

The structure can be sumarrized by the following graph:

![twitter-example](image/twitter-example.jpg)

With that set, let's define a new method `initTweetOntology` inside `GraknTweetOntologyHelper` class and define our ontology creation there.

```java
public class GraknTweetOntologyHelper {
  public static void initTweetOntology(GraknGraph graknGraph) {

  }
}
```

Start by defining our resources:

```java
// resources
ResourceType idType = graknGraph.putResourceType("identifier", ResourceType.DataType.STRING);
ResourceType textType = graknGraph.putResourceType("text", ResourceType.DataType.STRING);
ResourceType screenNameType = graknGraph.putResourceType("screen_name", ResourceType.DataType.STRING);
```

Entities:

```java
// entities
EntityType tweetType = graknGraph.putEntityType("tweet");
EntityType userType = graknGraph.putEntityType("user");
```

Roles and relations:

```java
// roles
RoleType postsType = graknGraph.putRoleType("posts");
RoleType postedByType = graknGraph.putRoleType("posted_by");

// relations
RelationType userTweetRelationType = graknGraph.putRelationType("user-tweet-relation").relates(postsType).relates(postedByType);
```

And finally, assign resources and roles appropriately.

```java
// resource and relation assignments
tweetType.resource(idType);
tweetType.resource(textType);
userType.resource(screenNameType);
userType.plays(postsType);
tweetType.plays(postedByType);
```

Now invoke the method in `main` so the ontology is created at the start of the application.

```java
public static void main(String[] args) {
  try (GraknSession session = Grakn.session(graphImplementation, keyspace)) {
    withGraknGraph(session, graknGraph -> initTweetOntology(graknGraph)); // initialize ontology
  }
}
```

## Streaming Data From Twitter

Now that we're done with ontology creation, let's develop the code for listening to the public tweet stream. 

Define a new method `listenToTwitterStreamAsync ` and put it in a class named `AsyncTweetStreamProcessorHelper `.  In addition to accepting Twitter credential settings, we will also need to supply a callback `onTweetReceived`, will be invoked whenever the application receives a new tweet. Further down, we will use this callback for storing, querying and displaying tweets as they come.

```java
public class AsyncTweetStreamProcessorHelper {
  public static TwitterStream listenToTwitterStreamAsync(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, BiConsumer<String, String> onTweetReceived) {
    final String DEFAULT_LANGUAGE = "en";
    ...
  }
}
```

The first thing we need to do here is to create a `Configuration` object out of the Twitter credential settings. Let's write a dedicated method just for that and name it `createTwitterConfiguration`. Afterwards, use that method to create the `Configuration` object which we will need in `listenToTwitterStreamAsync`.

```java
public class AsyncTweetStreamProcessorHelper {
  public static TwitterStream listenToTwitterStreamAsync(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, BiConsumer<String, String> onTweetReceived) {
    final String DEFAULT_LANGUAGE = "en";
    Configuration conf = createTwitterConfiguration(consumerKey, consumerSecret, accessToken, accessTokenSecret);
    // ...
  }
  
  private static Configuration createTwitterConfiguration(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
    return new ConfigurationBuilder()
        .setDebugEnabled(false)
        .setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret)
        .build();
  }
}
```

Next we will create a private class `TweetListener` and make it implement the `StatusListener` interface from Twitter4J. This interface has a bunch of methods we can override depending on what we want to receive from Twitter. As we only care about receiving tweet updates and nothing else, we only need to override a single method `onStatus`.

The constructor of our `TweetListener` class accepts a callback `onStatusReceived` which will be executed  every time we receive a new tweet.

Once we're done defining the class let's come back to `listenToTwitterStreamAsync` and instantiate it. We will also instantiate two other classes, `TwitterStreamFactory` and `TwitterStream`. Now we can start listening to Twitter by calling the `sample` method. We supplied `"en"` which means we are only interested in English tweets.

```java
public class AsyncTweetStreamProcessorHelper {
  public static TwitterStream listenToTwitterStreamAsync(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, BiConsumer<String, String> onTweetReceived) {
    final String DEFAULT_LANGUAGE = "en";

    Configuration conf = createTwitterConfiguration(consumerKey, consumerSecret, accessToken, accessTokenSecret);
    TweetListener tweetListener = new TweetListener(onTweetReceived);

    TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(conf);
    TwitterStream twitterStreamSingleton = twitterStreamFactory.getInstance();
    twitterStreamSingleton.addListener(tweetListener);

    twitterStreamSingleton.sample(DEFAULT_LANGUAGE);

    return twitterStreamSingleton;
  }

// An implementation which implements twitter4j's StatusListener
class TweetListener implements StatusListener {
  public TweetListener(BiConsumer<String, String> onStatusReceived) {
    this.onStatusReceived = onStatusReceived;
  }

  public void onStatus(Status status) {
    onStatusReceived.accept(status.getUser().getScreenName(), status.getText());
  }
  public void onException(Exception ex) {
    ex.printStackTrace();
  }

  // a bunch of empty event handler implementations, we're not using them
  public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
  public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
  public void onScrubGeo(long lat, long long_) {}
  public void onStallWarning(StallWarning stallWarning) {}

  private BiConsumer<String, String> onStatusReceived;
}
```

Let's wrap up this section by adding the call to `listenToTwitterStreamAsync` into `main`.

```java
public static void main(String[] args) {
  try (GraknSession session = Grakn.session(graphImplementation, keyspace)) {
    withGraknGraph(session, graknGraph -> initTweetOntology(graknGraph)); // initialize ontology

    listenToTwitterStreamAsync(consumerKey, consumerSecret, accessToken, accessTokenSecret, (screenName, tweet) -> {
      // TODO: do something upon receiving a new tweet
    });
  }
}
```

## Inserting Tweets Into The Knowledge Graph

At this point our little program already has a clearly defined ontology, and is able to listen to incoming tweets. However, we have yet to decide what exactly we're going to do with them. In this section we will have a look at how to:

1. Insert an incoming tweet into the knowledge graph
2. Insert a user who posted the tweet, only once — we don't want to insert the same user twice
3. Maintain an association between a tweet and the user

We will be using the graph API for inserting the data in the graph because it is lightweight and efficient.

### Insert A Tweet

To insert a tweet, we must create a `tweet` entity and a `text` resource to hold the tweet's textual data, before associating said resource with the entity.

Let's do that with a new method. It will accept a single `String` and inserts it into the knowledge graph, before returning the `Entity` of said tweet.

Pay attention to how we need to retrieve the `EntityTypes` and `ResourceTypes` of entity and resource we are interested in — we need them in order to perform the actual insertion.

```java
public static Entity insertTweet(GraknGraph graknGraph, String tweet) {
    EntityType tweetEntityType = graknGraph.getEntityType("tweet");
    ResourceType tweetResouceType = graknGraph.getResourceType("text");

    Entity tweetEntity = tweetEntityType.addEntity();
    Resource tweetResource = tweetResouceType.putResource(tweet);

    return tweetEntity.resource(tweetResource);
  }
```

### Insert A User

In addition to the tweet, we also want to store who posted the tweet. A semantic we need to enforce is to insert a particular user only once, i.e., it doesn't make sense to store the same user twice.

Therefore, let's add a method for checking whether we've previously stored a particular user. We will be using Java 8's `Optional<T>`, where we return the `Entity` object of that user only if it exists in the knowledge graph. Otherwise, an `Optional.empty()` will be returned.

```java
public static Optional<Entity> findUser(QueryBuilder queryBuilder, String user) {
  MatchQuery findUser = queryBuilder.match(var("x").isa("user").has("screen_name", user)).limit(1);
  Iterator<Concept> concepts = findUser.get("x").iterator();
  if (concepts.hasNext()) {
    Entity entity = concepts.next().asEntity();
    return Optional.of(entity);
  }
  else return Optional.empty();
}
```

And the following method for inserting a user. This one is quite similar to the one we made for inserting a tweet.

```java
public static Entity insertUser(GraknGraph graknGraph, String user) {
  EntityType userEntityType = graknGraph.getEntityType("user");
  ResourceType userResourceType = graknGraph.getResourceType("screen_name");
  Entity userEntity = userEntityType.addEntity();
  Resource userResource = userResourceType.putResource(user);
  return userEntity.resource(userResource);
}
```

And finally, write a function for inserting a user only if it's not yet there in the knowledge graph.

```java
public static Entity insertUserIfNotExist(GraknGraph graknGraph, String screenName) {
  QueryBuilder qb = graknGraph.graql();
  return findUser(qb, screenName).orElse(insertUser(graknGraph, screenName));
}
```

### Relating The Tweet To The User

We're almost there with a complete tweet insertion functionality! There's only one thing left to do which is to relate the `tweet` entity with the `user` entity. Preserving this connection is crucial, after all.

The following function will create a relation between the user and tweet that we specify.

```java
public static Relation insertUserTweetRelation(GraknGraph graknGraph, Entity user, Entity tweet) {
  RelationType userTweetRelationType = graknGraph.getRelationType("user-tweet-relation");
  RoleType postsType = graknGraph.getRoleType("posts");
  RoleType postedByType = graknGraph.getRoleType("posted_by");

  Relation userTweetRelation = userTweetRelationType.addRelation()
      .addRolePlayer(postsType, user)
      .addRolePlayer(postedByType, tweet);

  return userTweetRelation;
}
```

### Tweet Insertion Wrap Up

Finally, let's wrap up by defining a function of which the sole responsibility is to execute all of the methods we have defined above.

```java
public static Relation insertUserTweet(GraknGraph graknGraph, String screenName, String tweet) {
  Entity tweetEntity = insertTweet(graknGraph, tweet);
  Entity userEntity = insertUserIfNotExist(graknGraph, screenName);
  return insertUserTweetRelation(graknGraph, userEntity, tweetEntity);
}
```

We're done with tweet insertion functionality! Next step: querying the stored data. Before we proceed, let's add the method we've just defined to the main method as shown below.

```java
public static void main(String[] args) {
  try (GraknSession session = Grakn.session(graphImplementation, keyspace)) {
    withGraknGraph(session, graknGraph -> initTweetOntology(graknGraph)); // initialize ontology

    listenToTwitterStreamAsync(consumerKey, consumerSecret, accessToken, accessTokenSecret, (screenName, tweet) -> {
      withGraknGraph(session, graknGraph -> insertUserTweet(graknGraph, screenName, tweet)); // insert tweet
      // TODO: perform some meaningful queries on the inserted data
    });
  }

```

## Crafting Simple Queries Using GraQL

We will perform a query which will count the number of tweets a user has posted since the program started. It can be achieved it by utilizing the aggregate query feature. Graql has been chosen over the graph API for this task because it is declarative and therefore much easier to use for complex queries than the graph API.

Let's look at how we can build it step-by-step, start by creating a `QueryBuilder` object which we will use to craft the query in GraQL.

```java
QueryBuilder qb = graknGraph.graql();
```

Now let's begin crafting the query. For this tutorial, let's create a `match` query where we retrieve both the `user` and `tweet`.

We will bind them into `var`s which will be named... `user` and `tweet`, respectively. Notice how we deliberately assign the `var`s  identical names as the respective entity types. This is not a necessity and in practice, you are free to name them anything you want.

Also, pay attention to how we also supply the `user-tweet-relation` relation as part of the condition.

```java
qb.match(
  var("user").isa("user"),
  var("tweet").isa("tweet"),
  var().rel("posts", "user").rel("posted_by", "tweet").isa("user-tweet-relation"))
```

The query we've just defined will return every user and tweet along with their relations. We will use it as the basis of the aggregate query. 

Let's do some aggregation over the result here. We will supply `"user"` and `count()` as the argument for `group()`, which essentially tells Grakn to group the result by username, and count the number of occurences per username.

```java
qb.match(
  var("user").isa("user"),
  var("tweet").isa("tweet"),
  var().rel("posts", "user").rel("posted_by", "tweet").isa("user-tweet-relation")
).aggregate(group("user", count()));
```

 The query will now return "the number of tweet a user has posted", which is what we want, as an object of type `Map<Concept, Long>`. To be able to conveniently iterate, we will transform it into the relatively more straightforward type `Stream<Map.Entry<String, Long>>`, i.e., a stream of pairs of username and tweet count.

```java
  // execute query
  Map<Concept, Long> result = ((Map<Concept, Long>) q.execute());

  // map Map<Concept, Long> into Stream<Map.Entry<String, Long>> before returning
  ResourceType screenNameResourceType = graknGraph.getResourceType("screen_name");

  Stream<Map.Entry<String, Long>> mapped = result.entrySet().stream().map(entry -> {
    Concept key = entry.getKey();
    Long value = entry.getValue();
    String screenName = (String) key.asEntity().resources(screenNameResourceType).iterator().next().getValue();
    return new HashMap.SimpleImmutableEntry<>(screenName, value);
  });
```

Voila! Here's how `calculateTweetCountPerUser` should look like.

```java
public static Stream<Map.Entry<String, Long>> calculateTweetCountPerUser(GraknGraph graknGraph) {
  // build query
  QueryBuilder qb = graknGraph.graql();
  AggregateQuery q = qb.match(
      var("user").isa("user"),
      var("tweet").isa("tweet"),
      var().rel("posts", "user").rel("posted_by", "tweet").isa("user-tweet-relation")
      ).aggregate(group("user", count()));

  // execute query
  Map<Concept, Long> result = ((Map<Concept, Long>) q.execute());

  // map Map<Concept, Long> into Stream<Map.Entry<String, Long>> before returning
  ResourceType screenNameResourceType = graknGraph.getResourceType("screen_name");

  Stream<Map.Entry<String, Long>> mapped = result.entrySet().stream().map(entry -> {
    Concept key = entry.getKey();
    Long value = entry.getValue();
    String screenName = (String) key.asEntity().resources(screenNameResourceType).iterator().next().getValue();
    return new HashMap.SimpleImmutableEntry<>(screenName, value);
  });

  return mapped;
}
```

With that done, let's update our `main` like so. We've introduced two changes here. First we've added the call to our newly made function `calculateTweetCountPerUser`. Second, we're adding a pretty print function `prettyPrintQueryResult` to display our query in a nice way.

```java
public class Main {
  // Twitter credentials
  private static final String consumerKey = "...";
  private static final String consumerSecret = "...";
  private static final String accessToken = "...";
  private static final String accessTokenSecret = "...";

  // Grakn settings
  private static final String graphImplementation = Grakn.IN_MEMORY;
  private static final String keyspace = "twitter-example";

  public static void main(String[] args) {
    try (GraknSession session = Grakn.session(graphImplementation, keyspace)) {
      withGraknGraph(session, graknGraph -> initTweetOntology(graknGraph)); // initialize ontology
  
      listenToTwitterStreamAsync(consumerKey, consumerSecret, accessToken, accessTokenSecret, (screenName, tweet) -> {
        withGraknGraph(session, graknGraph -> {
          insertUserTweet(graknGraph, screenName, tweet); // insert tweet
          Stream<Map.Entry<String, Long>> result = calculateTweetCountPerUser(graknGraph); // query
          prettyPrintQueryResult(result); // display
        });
      });
    }
  }

  public static void prettyPrintQueryResult(Stream<Map.Entry<String, Long>> result) {
    System.out.println("------");
    result.forEach(e -> System.out.println("-- user " + e.getKey() + " tweeted " + e.getValue() + " time(s)."));
    System.out.println("------");
  }
}
```

## Running The Application

We're all set! The only thing left is to run the application. Let's do it right away.

```sh
mvn package
java -jar target/twitterexample-1.0-SNAPSHOT.jar
```

Watch the terminal as the application runs. You should see the following text printed everytime there's an incoming tweet:

```
------
-- user knowl3dg3 tweeted 2 time(s).
-- user gr4ph tweeted 1 time(s).
-- user w1th tweeted 1 time(s).
-- user gr4kn tweeted 1 time(s).
------
```

Finally, we have shown you many useful concepts — from creating an ontology, storing data, crafting a GraQL query, as well as displaying the result of the query. These are fundamental concepts that you will likely use in almost every area.