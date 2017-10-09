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

import ai.grakn.concept.ConceptId;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ai.grakn.SNB.$author;
import static ai.grakn.SNB.$author1;
import static ai.grakn.SNB.$author2;
import static ai.grakn.SNB.$authorId;
import static ai.grakn.SNB.$birthday;
import static ai.grakn.SNB.$browserUsed;
import static ai.grakn.SNB.$comment;
import static ai.grakn.SNB.$commentId;
import static ai.grakn.SNB.$content;
import static ai.grakn.SNB.$creationDate;
import static ai.grakn.SNB.$date;
import static ai.grakn.SNB.$firstName;
import static ai.grakn.SNB.$forum;
import static ai.grakn.SNB.$forumId;
import static ai.grakn.SNB.$friend;
import static ai.grakn.SNB.$friendId;
import static ai.grakn.SNB.$gender;
import static ai.grakn.SNB.$lastName;
import static ai.grakn.SNB.$locationIp;
import static ai.grakn.SNB.$message;
import static ai.grakn.SNB.$messageId;
import static ai.grakn.SNB.$mod;
import static ai.grakn.SNB.$modId;
import static ai.grakn.SNB.$opId;
import static ai.grakn.SNB.$originalPost;
import static ai.grakn.SNB.$person;
import static ai.grakn.SNB.$personId;
import static ai.grakn.SNB.$place;
import static ai.grakn.SNB.$placeId;
import static ai.grakn.SNB.$title;
import static ai.grakn.SNB.birthday;
import static ai.grakn.SNB.browserUsed;
import static ai.grakn.SNB.childMessage;
import static ai.grakn.SNB.content;
import static ai.grakn.SNB.creationDate;
import static ai.grakn.SNB.creator;
import static ai.grakn.SNB.firstName;
import static ai.grakn.SNB.forumId;
import static ai.grakn.SNB.forumMember;
import static ai.grakn.SNB.friend;
import static ai.grakn.SNB.gender;
import static ai.grakn.SNB.groupForum;
import static ai.grakn.SNB.has;
import static ai.grakn.SNB.hasCreator;
import static ai.grakn.SNB.hasModerator;
import static ai.grakn.SNB.imageFile;
import static ai.grakn.SNB.isLocatedIn;
import static ai.grakn.SNB.key;
import static ai.grakn.SNB.knows;
import static ai.grakn.SNB.lastName;
import static ai.grakn.SNB.located;
import static ai.grakn.SNB.locationIp;
import static ai.grakn.SNB.memberMessage;
import static ai.grakn.SNB.message;
import static ai.grakn.SNB.messageId;
import static ai.grakn.SNB.moderated;
import static ai.grakn.SNB.moderator;
import static ai.grakn.SNB.original;
import static ai.grakn.SNB.originalPost;
import static ai.grakn.SNB.parentMessage;
import static ai.grakn.SNB.person;
import static ai.grakn.SNB.personId;
import static ai.grakn.SNB.placeId;
import static ai.grakn.SNB.post;
import static ai.grakn.SNB.product;
import static ai.grakn.SNB.region;
import static ai.grakn.SNB.reply;
import static ai.grakn.SNB.replyOf;
import static ai.grakn.SNB.title;
import static ai.grakn.SNB.toEpoch;
import static ai.grakn.graql.Graql.ask;
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

                List<LdbcShortQuery2PersonPostsResult> result = allResults.stream()
                        .sorted(comparing(by($date)).thenComparing(by($messageId)).reversed())
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
    }

    private static <T> Function<Answer, T> by(Var var) {
        return map -> resource(map, var);
    }

    private static <T> T resource(Answer result, Var var) {
        return result.get(var).<T>asAttribute().getValue();
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

                List<Answer> results = graph.graql().match(
                        $person.has(personId, operation.personId()),
                        var().rel($person).rel($friend).isa(knows).has(creationDate, $date),
                        $friend.has(personId, $friendId).has(firstName, $firstName).has(lastName, $lastName)
                ).get().execute();

                List<LdbcShortQuery3PersonFriendsResult> result = results.stream()
                        .sorted(Comparator.comparing(by($date)).reversed().thenComparing(by($friendId)))
                        .map(map -> new LdbcShortQuery3PersonFriendsResult(
                                resource(map, $friendId),
                                resource(map, $firstName),
                                resource(map, $lastName),
                                toEpoch(resource(map, $date))
                        )).collect(Collectors.toList());

                resultReporter.report(0, result, operation);
            }
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

                List<Answer> results = graph.graql().match(
                        $message.has(messageId, operation.messageId()),
                        var().rel($message).rel($date).isa(has(creationDate)),
                        (var().rel($message).rel($content).isa(has(content))).or(var().rel($message).rel($content).isa(has(imageFile)))
                ).get().execute();

                if (results.size() > 0) {
                    Answer fres = results.get(0);

                    LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                            resource(fres, $content),
                            toEpoch(resource(fres, $date))
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

                List<Answer> results = graph.graql().match(
                        $message.has(messageId, operation.messageId()),
                        var().rel(product, $message).rel(creator, $person).isa(hasCreator),
                        var().rel($person, $firstName).isa(has(firstName)),
                        var().rel($person, $lastName).isa(has(lastName)),
                        var().rel($person, $personId).isa(key(personId))
                ).get().execute();

                if (results.size() >= 1) {
                    Answer fres = results.get(0);
                    LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                            resource(fres, $personId),
                            resource(fres, $firstName),
                            resource(fres, $lastName)
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

                List<Answer> results = graph.graql().infer(true).match(
                        $message.has(messageId, operation.messageId()),
                        var().rel(memberMessage, $message).rel(groupForum, $forum).isa(forumMember),
                        $forum.has(forumId, $forumId).has(title, $title),
                        var().rel(moderated, $forum).rel(moderator, $mod).isa(hasModerator),
                        $mod.isa(person).has(personId, $modId).has(firstName, $firstName).has(lastName, $lastName)
                ).get().execute();

                if (results.size() > 0) {
                    Answer fres = results.get(0);
                    LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                            resource(fres, $forumId),
                            resource(fres, $title),
                            resource(fres, $modId),
                            resource(fres, $firstName),
                            resource(fres, $lastName)
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


                List<Answer> results = graph.graql().match(
                        $message.isa(message).has(messageId, operation.messageId()),
                        var().rel(product, $message).rel(creator, $author1).isa(hasCreator),
                        var().rel(original, $message).rel(reply, $comment).isa(replyOf),
                        var().rel($comment).rel($commentId).isa(key(messageId)),
                        var().rel($comment).rel($content).isa(has(content)),
                        var().rel($comment).rel($date).isa(has(creationDate)),
                        var().rel(product, $comment).rel(creator, $author2).isa(hasCreator),
                        var().rel($author2, $personId).isa(key(personId)),
                        var().rel($author2, $firstName).isa(has(firstName)),
                        var().rel($author2, $lastName).isa(has(lastName))
                ).get().execute();

                List<LdbcShortQuery7MessageRepliesResult> result = results.stream()
                        .sorted(Comparator.comparing(by($date)).reversed().thenComparing(by($personId)))
                        .map(map -> new LdbcShortQuery7MessageRepliesResult(
                                resource(map, $commentId),
                                resource(map, $content),
                                toEpoch(resource(map, $date)),
                                resource(map, $personId),
                                resource(map, $firstName),
                                resource(map, $lastName),
                                checkIfFriends(conceptId(map, $author1), conceptId(map, $author2), graph)))
                        .collect(Collectors.toList());

                resultReporter.report(0, result, operation);

            }

        }

        private boolean checkIfFriends(ConceptId author1, ConceptId author2, GraknTx graph) {
            return graph.graql().match(
                    var().rel(friend, var().id(author1)).rel(friend, var().id(author2)).isa(knows)
            ).aggregate(ask()).execute();
        }

        private ConceptId conceptId(Answer result, Var var) {
            return result.get(var).getId();
        }
    }
}