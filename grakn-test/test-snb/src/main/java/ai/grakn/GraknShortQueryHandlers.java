/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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
package ai.grakn;

import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Order;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.SNB.$author;
import static ai.grakn.SNB.$authorId;
import static ai.grakn.SNB.$birthday;
import static ai.grakn.SNB.$browserUsed;
import static ai.grakn.SNB.$content;
import static ai.grakn.SNB.$creationDate;
import static ai.grakn.SNB.$date;
import static ai.grakn.SNB.$firstName;
import static ai.grakn.SNB.$gender;
import static ai.grakn.SNB.$lastName;
import static ai.grakn.SNB.$locationIp;
import static ai.grakn.SNB.$message;
import static ai.grakn.SNB.$messageId;
import static ai.grakn.SNB.$opId;
import static ai.grakn.SNB.$originalPost;
import static ai.grakn.SNB.$person;
import static ai.grakn.SNB.$place;
import static ai.grakn.SNB.$placeId;
import static ai.grakn.SNB.birthday;
import static ai.grakn.SNB.browserUsed;
import static ai.grakn.SNB.childMessage;
import static ai.grakn.SNB.content;
import static ai.grakn.SNB.creationDate;
import static ai.grakn.SNB.creator;
import static ai.grakn.SNB.firstName;
import static ai.grakn.SNB.gender;
import static ai.grakn.SNB.has;
import static ai.grakn.SNB.hasCreator;
import static ai.grakn.SNB.imageFile;
import static ai.grakn.SNB.isLocatedIn;
import static ai.grakn.SNB.key;
import static ai.grakn.SNB.lastName;
import static ai.grakn.SNB.located;
import static ai.grakn.SNB.locationIp;
import static ai.grakn.SNB.messageId;
import static ai.grakn.SNB.originalPost;
import static ai.grakn.SNB.parentMessage;
import static ai.grakn.SNB.person;
import static ai.grakn.SNB.personId;
import static ai.grakn.SNB.placeId;
import static ai.grakn.SNB.post;
import static ai.grakn.SNB.product;
import static ai.grakn.SNB.region;
import static ai.grakn.SNB.toEpoch;
import static ai.grakn.graql.Graql.var;
import static java.util.Comparator.comparing;

/**
 * Implementations of the LDBC SNB short queries.
 *
 * @author sheldon, miko, felix
 */
public class GraknShortQueryHandlers {

    /**
     * Short Query 1
     */
    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                Optional<Answer> answer = graph.graql().match(
                        $person.has(personId, operation.personId()),
                        var().rel($person).rel($firstName).isa(has(firstName)),
                        var().rel($person).rel($lastName).isa(has(lastName)),
                        var().rel($person).rel($birthday).isa(has(birthday)),
                        var().rel($person).rel($locationIp).isa(has(locationIp)),
                        var().rel($person).rel($browserUsed).isa(has(browserUsed)),
                        var().rel($person).rel($gender).isa(has(gender)),
                        var().rel($person).rel($creationDate).isa(has(creationDate)),
                        var().rel(located, $person).rel(region, $place).isa(isLocatedIn),
                        var().rel($place).rel($placeId).isa(key(placeId))
                ).get().stream().findAny();

                if (answer.isPresent()) {
                    Answer fres = answer.get();

                    LdbcShortQuery1PersonProfileResult result =
                            new LdbcShortQuery1PersonProfileResult(
                                    fres.get($firstName).<String>asAttribute().getValue(),
                                    fres.get($lastName).<String>asAttribute().getValue(),
                                    toEpoch(fres.get($birthday).<LocalDateTime>asAttribute().getValue()),
                                    fres.get($locationIp).<String>asAttribute().getValue(),
                                    fres.get($browserUsed).<String>asAttribute().getValue(),
                                    fres.get($placeId).<Long>asAttribute().getValue(),
                                    fres.get($gender).<String>asAttribute().getValue(),
                                    toEpoch(fres.get($creationDate).<LocalDateTime>asAttribute().getValue()));

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);
                }
            }
        }
    }

    /**
     * Short Query 2
     */
    // The following requires a rule to properly work
    public static class LdbcShortQuery2PersonPostsHandler implements
            OperationHandler<LdbcShortQuery2PersonPosts, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                List<Answer> messageResults = graph.graql().match(
                        $person.isa(person).has(personId, operation.personId()),
                        var().rel(creator, $person).rel(product, $message).isa(hasCreator),
                        var().rel($message).rel($date).isa(has(creationDate)),
                        var().rel($message).rel($messageId).isa(key(messageId))
                ).orderBy($date, Order.desc).limit(operation.limit()).get().execute();

                List<Answer> allResults = new ArrayList<>();
                messageResults.forEach(a -> {

                    List<Answer> results = graph.graql().infer(true).match(
                            $message.id(a.get($message).getId()),
                            var().rel($message).rel($date).isa(has(creationDate)),
                            var().rel($message).rel($messageId).isa(key(messageId)),
                            (var().rel($message).rel($content).isa(has(content))).or(var().rel($message).rel($content).isa(has(imageFile))),
                            $originalPost.isa(post),
                            var().rel(childMessage, $message).rel(parentMessage, $originalPost).isa(originalPost),
                            var().rel($originalPost).rel($opId).isa(key(messageId)),
                            $author.isa(person),
                            var().rel(product, $originalPost).rel(creator, $author).isa(hasCreator),
                            var().rel($author).rel($authorId).isa(key(personId)),
                            var().rel($author).rel($firstName).isa(has(firstName)),
                            var().rel($author).rel($lastName).isa(has(lastName))
                    ).get().execute();

                    allResults.addAll(results);
                });

                Function<Answer, Long> byDate = map -> toEpoch(resource(map, $date));
                Function<Answer, Long> byMessageId = map -> resource(map, $messageId);

                List<LdbcShortQuery2PersonPostsResult> result = allResults.stream()
                        .sorted(comparing(byDate).thenComparing(byMessageId).reversed())
                        .map(map -> new LdbcShortQuery2PersonPostsResult(resource(map, $messageId),
                                resource(map, $content),
                                toEpoch(resource(map, $date)),
                                resource(map, $opId),
                                resource(map, $authorId),
                                resource(map, $firstName),
                                resource(map, $lastName)))
                        .collect(Collectors.toList());

                resultReporter.report(0, result, operation);

            }

        }

        private <T> T resource(Answer result, Var var) {
            return result.get(var).<T>asAttribute().getValue();
        }
    }


    /**
     * Short Query 3
     */
    public static class LdbcShortQuery3PersonFriendsHandler implements
            OperationHandler<LdbcShortQuery3PersonFriends, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$person has person-id " + operation.personId() + "; " +
                        "$rel ($person, $friend) isa knows; " +
                        "$rel has creation-date  $date; " +
                        "$friend has person-id $friendId has first-name $fname has last-name $lname; get;";


                List<Answer> results = graph.graql().<GetQuery>parse(query).execute();


                    Comparator<Answer> ugly = Comparator.<Answer>comparingLong(map -> map.get("date").<LocalDateTime>asAttribute().getValue().toInstant(ZoneOffset.UTC).toEpochMilli()).reversed()
                            .thenComparing(map -> resource(map, "friendId"));

                    List<LdbcShortQuery3PersonFriendsResult> result = results.stream()
                            .sorted(ugly)
                            .map(map -> new LdbcShortQuery3PersonFriendsResult(resource(map, "friendId"),
                                    resource(map, "fname"),
                                    resource(map, "lname"),
                                    map.get("date").<LocalDateTime>asAttribute().getValue().toInstant(ZoneOffset.UTC).toEpochMilli()))
                            .collect(Collectors.toList());

                    resultReporter.report(0, result, operation);


            }
        }

        private <T> T resource(Answer result, String name) {
            return result.get(name).<T>asAttribute().getValue();
        }
    }


    /**
     * Short Query 4
     */
    public static class LdbcShortQuery4MessageContentHandler implements
            OperationHandler<LdbcShortQuery4MessageContent, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$m has message-id " + operation.messageId() + "; " +
                        "($m, $date) isa has-creation-date; " +
                        "($m, $content) isa has-content or ($m, $content) isa has-image-file; get;";

                List<Answer> results = graph.graql().<GetQuery>parse(query).execute();


                if (results.size() > 0) {
                    Answer fres = results.get(0);

                    LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                            (String) fres.get("content").asAttribute().getValue(),
                            ((LocalDateTime) fres.get("date").asAttribute().getValue()).toInstant(ZoneOffset.UTC).toEpochMilli()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }


    }

    /**
     * Short Query 5
     */
    public static class LdbcShortQuery5MessageCreatorHandler implements
            OperationHandler<LdbcShortQuery5MessageCreator, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        " $m has message-id " + operation.messageId() + ";" +
                        " (product: $m , creator: $person) isa has-creator;" +
                        " ($person, $fname) isa has-first-name;" +
                        " ($person, $lname) isa has-last-name;" +
                        " ($person, $pID) isa key-person-id; get;";

                List<Answer> results = graph.graql().<GetQuery>parse(query).execute();

                if (results.size() >= 1) {
                    Answer fres = results.get(0);
                    LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                            (Long) fres.get("pID").asAttribute().getValue(),
                            (String) fres.get("fname").asAttribute().getValue(),
                            (String) fres.get("lname").asAttribute().getValue()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }
    }


    /**
     * Short Query 6
     */
    // The following requires a rule to properly work
    public static class LdbcShortQuery6MessageForumHandler implements
            OperationHandler<LdbcShortQuery6MessageForum, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                String query = "match " +
                        "$m has message-id " + operation.messageId() + "; " +
                        "(member-message: $m , group-forum: $forum) isa forum-member;" +
                        "$forum has forum-id $fid has title $title; " +
                        "(moderated: $forum, moderator: $mod) isa has-moderator; " +
                        "$mod isa person has person-id $modid has first-name $fname has last-name $lname; get;";


                List<Answer> results = graph.graql().infer(true).<GetQuery>parse(query).execute();

                if (results.size() > 0) {
                    Answer fres = results.get(0);
                    LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                            (Long) fres.get("fid").asAttribute().getValue(),
                            (String) fres.get("title").asAttribute().getValue(),
                            (Long) fres.get("modid").asAttribute().getValue(),
                            (String) fres.get("fname").asAttribute().getValue(),
                            (String) fres.get("lname").asAttribute().getValue()
                    );

                    resultReporter.report(0, result, operation);

                } else {
                    resultReporter.report(0, null, operation);

                }
            }
        }

        }


    /**
     * Short Query 7
     */
    public static class LdbcShortQuery7MessageRepliesHandler implements
            OperationHandler<LdbcShortQuery7MessageReplies, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.READ)) {

                String query = "match $m isa message has message-id " + operation.messageId() + " ;" +
                        "(product: $m, creator: $author1) isa has-creator; " +
                        "(original: $m, reply: $comment) isa reply-of; " +
                        "($comment, $cid) isa key-message-id; " +
                        "($comment, $content) isa has-content; " +
                        "($comment, $date) isa has-creation-date; " +
                        "(product: $comment, creator: $author2) isa has-creator; " +
                        "($author2, $pid) isa key-person-id; " +
                        "($author2, $fname) isa has-first-name; " +
                        "($author2, $lname) isa has-last-name; get;";

                List<Answer> results = graph.graql().<GetQuery>parse(query).execute();

                Comparator<Answer> ugly = Comparator.<Answer>comparingLong(map -> map.get("date").<LocalDateTime>asAttribute().getValue().toInstant(ZoneOffset.UTC).toEpochMilli()).reversed()
                        .thenComparing(map -> resource(map, "pid"));

                List<LdbcShortQuery7MessageRepliesResult> result = results.stream()
                        .sorted(ugly)
                        .map(map -> new LdbcShortQuery7MessageRepliesResult(resource(map, "cid"),
                                resource(map, "content"),
                                map.get("date").<LocalDateTime>asAttribute().getValue().toInstant(ZoneOffset.UTC).toEpochMilli(),
                                resource(map, "pid"),
                                resource(map, "fname"),
                                resource(map, "lname"),
                                checkIfFriends(conceptId(map, "author1"), conceptId(map, "author2"), graph)))
                        .collect(Collectors.toList());

                resultReporter.report(0, result, operation);

            }

        }

        private boolean checkIfFriends(String author1, String author2, GraknTx graph) {
            String query = "match $x id '" + author1 + "'; $y id '" + author2 + "';" +
                    "(friend: $x, friend:  $y) isa knows; aggregate ask;";
            return graph.graql().<AggregateQuery<Boolean>>parse(query).execute();
        }

        private String conceptId(Answer result, String name) {
            return result.get(name).getId().toString();
        }

        private <T> T resource(Answer result, String name) {
            return result.get(name).<T>asAttribute().getValue();
        }
    }
}