package ai.grakn;

import ai.grakn.graql.AskQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.admin.Answer;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by miko on 06/04/2017.
 * Ruined by felix on 06/04/2017.
 */


public class GraknShortQueryHandlers {

    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query =
                        "match" +
                                "$person has person-id " +
                                operation.personId() + "; " +
                                "($person, $first-name) isa has-first-name; " +
                                "($person, $last-name) isa has-last-name; " +
                                "($person, $birth-day) isa has-birth-day; " +
                                "($person, $location-ip) isa has-location-ip; " +
                                "($person, $browser-used) isa has-browser-used; " +
                                "($person, $gender) isa has-gender; " +
                                "($person, $creation-date) isa has-creation-date; " +
                                "(located: $person, region: $place) isa is-located-in; " +
                                "($place, $placeID) isa key-place-id; ";

                List<Answer> results = graph.graql().<MatchQuery>parse(query).execute();
                if (results.size() > 0) {
                    Answer fres = results.get(0);

                    LdbcShortQuery1PersonProfileResult result =
                            new LdbcShortQuery1PersonProfileResult(
                                    (String) fres.get("first-name").asResource().getValue(),
                                    (String) fres.get("last-name").asResource().getValue(),
                                    ((LocalDateTime) fres.get("birth-day").asResource().getValue()).toInstant(ZoneOffset.UTC).toEpochMilli(),
                                    (String) fres.get("location-ip").asResource().getValue(),
                                    (String) fres.get("browser-used").asResource().getValue(),
                                    (Long) fres.get("placeID").asResource().getValue(),
                                    (String) fres.get("gender").asResource().getValue(),
                                    ((LocalDateTime) fres.get("creation-date").asResource().getValue()).toInstant(ZoneOffset.UTC).toEpochMilli());

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);
                }
            }
        }
    }

    // The following requires a rule to properly work
    public static class LdbcShortQuery2PersonPostsHandler implements
            OperationHandler<LdbcShortQuery2PersonPosts, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String messageQuery = "match " +
                        "$person isa person has person-id " + operation.personId() + "; " +
                        "(creator: $person, product: $message) isa has-creator; " +
                        "($message, $date) isa has-creation-date ; " +
                        "($message, $messageId) isa key-message-id; " +
                        "order by $date desc;" +
                        "limit " + String.valueOf(operation.limit()) + ";";

                List<Answer> messageResults = graph.graql().<MatchQuery>parse(messageQuery).execute();

                List<Answer> allResults = new ArrayList<>();
                messageResults.forEach(a -> {

                    String query = "match " +
                            "$message id \"" + a.get("message").getId().toString() + "\"; " +
                            "($message, $date) isa has-creation-date ; " +
                            "($message, $messageId) isa key-message-id; " +
                            "($message, $content) isa has-content or ($message, $content) isa has-image-file; " +
                            "$originalPost isa post; " +
                            "(child-message: $message, parent-message: $originalPost) isa original-post; " +
                            "($originalPost, $opId) isa key-message-id; " +
                            "$person2 isa person; " +
                            "(product: $originalPost, creator: $person2) isa has-creator; " +
                            "($person2, $authorId) isa key-person-id; " +
                            "($person2, $fname) isa has-first-name; " +
                            "($person2, $lname) isa has-last-name; ";

                    List<Answer> results = graph.graql().infer(true).<MatchQuery>parse(query).execute();
                    allResults.addAll(results);
                });

                Comparator<Answer> ugly = Comparator.<Answer>comparingLong(map -> map.get("date").<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli())
                        .thenComparing(map -> resource(map, "messageId")).reversed();

                List<LdbcShortQuery2PersonPostsResult> result = allResults.stream()
                        .sorted(ugly)
                        .map(map -> new LdbcShortQuery2PersonPostsResult(resource(map, "messageId"),
                                resource(map, "content"),
                                map.get("date").<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli(),
                                resource(map, "opId"),
                                resource(map, "authorId"),
                                resource(map, "fname"),
                                resource(map, "lname")))
                        .collect(Collectors.toList());

                resultReporter.report(0, result, operation);

            }

        }

        private <T> T resource(Answer result, String name) {
            return result.get(name).<T>asResource().getValue();
        }
    }


    public static class LdbcShortQuery3PersonFriendsHandler implements
            OperationHandler<LdbcShortQuery3PersonFriends, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$person has person-id " + operation.personId() + "; " +
                        "$rel ($person, $friend) isa knows; " +
                        "$rel has creation-date  $date; " +
                        "$friend has person-id $friendId has first-name $fname has last-name $lname;";


                List<Answer> results = graph.graql().<MatchQuery>parse(query).execute();


                    Comparator<Answer> ugly = Comparator.<Answer>comparingLong(map -> map.get("date").<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli()).reversed()
                            .thenComparing(map -> resource(map, "friendId"));

                    List<LdbcShortQuery3PersonFriendsResult> result = results.stream()
                            .sorted(ugly)
                            .map(map -> new LdbcShortQuery3PersonFriendsResult(resource(map, "friendId"),
                                    resource(map, "fname"),
                                    resource(map, "lname"),
                                    map.get("date").<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli()))
                            .collect(Collectors.toList());

                    resultReporter.report(0, result, operation);


            }
        }

        private <T> T resource(Answer result, String name) {
            return result.get(name).<T>asResource().getValue();
        }
    }


    public static class LdbcShortQuery4MessageContentHandler implements
            OperationHandler<LdbcShortQuery4MessageContent, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$m has message-id " + operation.messageId() + "; " +
                        "($m, $date) isa has-creation-date; " +
                        "($m, $content) isa has-content or ($m, $content) isa has-image-file;";

                List<Answer> results = graph.graql().<MatchQuery>parse(query).execute();


                if (results.size() > 0) {
                    Answer fres = results.get(0);

                    LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                            (String) fres.get("content").asResource().getValue(),
                            ((LocalDateTime) fres.get("date").asResource().getValue()).toInstant(ZoneOffset.UTC).toEpochMilli()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }


    }

    public static class LdbcShortQuery5MessageCreatorHandler implements
            OperationHandler<LdbcShortQuery5MessageCreator, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        " $m has message-id " + operation.messageId() + ";" +
                        " (product: $m , creator: $person) isa has-creator;" +
                        " ($person, $fname) isa has-first-name;" +
                        " ($person, $lname) isa has-last-name;" +
                        " ($person, $pID) isa key-person-id;";

                List<Answer> results = graph.graql().<MatchQuery>parse(query).execute();

                if (results.size() >= 1) {
                    Answer fres = results.get(0);
                    LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                            (Long) fres.get("pID").asResource().getValue(),
                            (String) fres.get("fname").asResource().getValue(),
                            (String) fres.get("lname").asResource().getValue()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }
    }


    // The following requires a rule to properly work
    public static class LdbcShortQuery6MessageForumHandler implements
            OperationHandler<LdbcShortQuery6MessageForum, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$m has message-id " + operation.messageId() + "; " +
                        "(member-message: $m , group-forum: $forum) isa forum-member;" +
                        "$forum has forum-id $fid has title $title; " +
                        "(moderated: $forum, moderator: $mod) isa has-moderator; " +
                        "$mod isa person has person-id $modid has first-name $fname has last-name $lname;";


                List<Answer> results = graph.graql().infer(true).<MatchQuery>parse(query).execute();

                if (results.size() > 0) {
                    Answer fres = results.get(0);
                    LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                            (Long) fres.get("fid").asResource().getValue(),
                            (String) fres.get("title").asResource().getValue(),
                            (Long) fres.get("modid").asResource().getValue(),
                            (String) fres.get("fname").asResource().getValue(),
                            (String) fres.get("lname").asResource().getValue()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }

        }


    public static class LdbcShortQuery7MessageRepliesHandler implements
            OperationHandler<LdbcShortQuery7MessageReplies, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                String query = "match $m isa message has message-id " + operation.messageId() + " ;" +
                        "(product: $m, creator: $author1) isa has-creator; " +
                        "(original: $m, reply: $comment) isa reply-of; " +
                        "($comment, $cid) isa key-message-id; " +
                        "($comment, $content) isa has-content; " +
                        "($comment, $date) isa has-creation-date; " +
                        "(product: $comment, creator: $author2) isa has-creator; " +
                        "($author2, $pid) isa key-person-id; " +
                        "($author2, $fname) isa has-first-name; " +
                        "($author2, $lname) isa has-last-name;";

                List<Answer> results = graph.graql().<MatchQuery>parse(query).execute();

                Comparator<Answer> ugly = Comparator.<Answer>comparingLong(map -> map.get("date").<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli()).reversed()
                        .thenComparing(map -> resource(map, "pid"));

                List<LdbcShortQuery7MessageRepliesResult> result = results.stream()
                        .sorted(ugly)
                        .map(map -> new LdbcShortQuery7MessageRepliesResult(resource(map, "cid"),
                                resource(map, "content"),
                                map.get("date").<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli(),
                                resource(map, "pid"),
                                resource(map, "fname"),
                                resource(map, "lname"),
                                checkIfFriends(conceptId(map, "author1"), conceptId(map, "author2"), graph)))
                        .collect(Collectors.toList());

                resultReporter.report(0, result, operation);

            }

        }

        private boolean checkIfFriends(String author1, String author2, GraknGraph graph) {
            String query = "match $x id '" + author1 + "'; $y id '" + author2 + "';" +
                    "(friend: $x, friend:  $y) isa knows; ask;";
            return graph.graql().<AskQuery>parse(query).execute();
        }

        private String conceptId(Answer result, String name) {
            return result.get(name).getId().toString();
        }

        private <T> T resource(Answer result, String name) {
            return result.get(name).<T>asResource().getValue();
        }
    }
}