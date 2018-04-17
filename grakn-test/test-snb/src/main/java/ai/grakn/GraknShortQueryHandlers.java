/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
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
import java.util.List;
import java.util.Optional;
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
import static ai.grakn.SNB.BIRTHDAY;
import static ai.grakn.SNB.BROWSER_USED;
import static ai.grakn.SNB.CHILD_MESSAGE;
import static ai.grakn.SNB.by;
import static ai.grakn.SNB.CONTENT;
import static ai.grakn.SNB.CREATION_DATE;
import static ai.grakn.SNB.CREATOR;
import static ai.grakn.SNB.FIRST_NAME;
import static ai.grakn.SNB.FORUM_ID;
import static ai.grakn.SNB.FORUM_MEMBER;
import static ai.grakn.SNB.FRIEND;
import static ai.grakn.SNB.GENDER;
import static ai.grakn.SNB.GROUP_FORUM;
import static ai.grakn.SNB.has;
import static ai.grakn.SNB.HAS_CREATOR;
import static ai.grakn.SNB.HAS_MODERATOR;
import static ai.grakn.SNB.IMAGE_FILE;
import static ai.grakn.SNB.IS_LOCATED_IN;
import static ai.grakn.SNB.key;
import static ai.grakn.SNB.KNOWS;
import static ai.grakn.SNB.LAST_NAME;
import static ai.grakn.SNB.LOCATED;
import static ai.grakn.SNB.LOCATION_IP;
import static ai.grakn.SNB.MEMBER_MESSAGE;
import static ai.grakn.SNB.MESSAGE;
import static ai.grakn.SNB.MESSAGE_ID;
import static ai.grakn.SNB.MODERATED;
import static ai.grakn.SNB.MODERATOR;
import static ai.grakn.SNB.ORIGINAL;
import static ai.grakn.SNB.ORIGINAL_POST;
import static ai.grakn.SNB.PARENT_MESSAGE;
import static ai.grakn.SNB.PERSON;
import static ai.grakn.SNB.PERSON_ID;
import static ai.grakn.SNB.PLACE_ID;
import static ai.grakn.SNB.POST;
import static ai.grakn.SNB.PRODUCT;
import static ai.grakn.SNB.REGION;
import static ai.grakn.SNB.REPLY;
import static ai.grakn.SNB.REPLY_OF;
import static ai.grakn.SNB.resource;
import static ai.grakn.SNB.TITLE;
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

    private GraknShortQueryHandlers(){}

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
                        $person.has(PERSON_ID, operation.personId()),
                        var().rel($person).rel($firstName).isa(has(FIRST_NAME)),
                        var().rel($person).rel($lastName).isa(has(LAST_NAME)),
                        var().rel($person).rel($birthday).isa(has(BIRTHDAY)),
                        var().rel($person).rel($locationIp).isa(has(LOCATION_IP)),
                        var().rel($person).rel($browserUsed).isa(has(BROWSER_USED)),
                        var().rel($person).rel($gender).isa(has(GENDER)),
                        var().rel($person).rel($creationDate).isa(has(CREATION_DATE)),
                        var().rel(LOCATED, $person).rel(REGION, $place).isa(IS_LOCATED_IN),
                        var().rel($place).rel($placeId).isa(key(PLACE_ID))
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
                        $person.isa(PERSON).has(PERSON_ID, operation.personId()),
                        var().rel(CREATOR, $person).rel(PRODUCT, $message).isa(HAS_CREATOR),
                        var().rel($message).rel($date).isa(has(CREATION_DATE)),
                        var().rel($message).rel($messageId).isa(key(MESSAGE_ID))
                ).orderBy($date, Order.desc).limit(operation.limit()).get().execute();

                List<Answer> allResults = new ArrayList<>();
                messageResults.forEach(a -> {

                    List<Answer> results = graph.graql().infer(true).match(
                            $message.id(a.get($message).getId()),
                            var().rel($message).rel($date).isa(has(CREATION_DATE)),
                            var().rel($message).rel($messageId).isa(key(MESSAGE_ID)),
                            (var().rel($message).rel($content).isa(has(CONTENT))).or(var().rel($message).rel($content).isa(has(IMAGE_FILE))),
                            $originalPost.isa(POST),
                            var().rel(CHILD_MESSAGE, $message).rel(PARENT_MESSAGE, $originalPost).isa(ORIGINAL_POST),
                            var().rel($originalPost).rel($opId).isa(key(MESSAGE_ID)),
                            $author.isa(PERSON),
                            var().rel(PRODUCT, $originalPost).rel(CREATOR, $author).isa(HAS_CREATOR),
                            var().rel($author).rel($authorId).isa(key(PERSON_ID)),
                            var().rel($author).rel($firstName).isa(has(FIRST_NAME)),
                            var().rel($author).rel($lastName).isa(has(LAST_NAME))
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
                        $person.has(PERSON_ID, operation.personId()),
                        var().rel($person).rel($friend).isa(KNOWS).has(CREATION_DATE, $date),
                        $friend.has(PERSON_ID, $friendId).has(FIRST_NAME, $firstName).has(LAST_NAME, $lastName)
                ).get().execute();

                List<LdbcShortQuery3PersonFriendsResult> result = results.stream()
                        .sorted(comparing(by($date)).reversed().thenComparing(by($friendId)))
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
                        $message.has(MESSAGE_ID, operation.messageId()),
                        var().rel($message).rel($date).isa(has(CREATION_DATE)),
                        (var().rel($message).rel($content).isa(has(CONTENT))).or(var().rel($message).rel($content).isa(has(IMAGE_FILE)))
                ).get().execute();

                if (!results.isEmpty()) {
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
                        $message.has(MESSAGE_ID, operation.messageId()),
                        var().rel(PRODUCT, $message).rel(CREATOR, $person).isa(HAS_CREATOR),
                        var().rel($person).rel($firstName).isa(has(FIRST_NAME)),
                        var().rel($person).rel($lastName).isa(has(LAST_NAME)),
                        var().rel($person).rel($personId).isa(key(PERSON_ID))
                ).get().execute();

                if (!results.isEmpty()) {
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
                        $message.has(MESSAGE_ID, operation.messageId()),
                        var().rel(MEMBER_MESSAGE, $message).rel(GROUP_FORUM, $forum).isa(FORUM_MEMBER),
                        $forum.has(FORUM_ID, $forumId).has(TITLE, $title),
                        var().rel(MODERATED, $forum).rel(MODERATOR, $mod).isa(HAS_MODERATOR),
                        $mod.isa(PERSON).has(PERSON_ID, $modId).has(FIRST_NAME, $firstName).has(LAST_NAME, $lastName)
                ).get().execute();

                if (!results.isEmpty()) {
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
                        $message.isa(MESSAGE).has(MESSAGE_ID, operation.messageId()),
                        var().rel(PRODUCT, $message).rel(CREATOR, $author1).isa(HAS_CREATOR),
                        var().rel(ORIGINAL, $message).rel(REPLY, $comment).isa(REPLY_OF),
                        var().rel($comment).rel($commentId).isa(key(MESSAGE_ID)),
                        var().rel($comment).rel($content).isa(has(CONTENT)),
                        var().rel($comment).rel($date).isa(has(CREATION_DATE)),
                        var().rel(PRODUCT, $comment).rel(CREATOR, $author2).isa(HAS_CREATOR),
                        var().rel($author2).rel($personId).isa(key(PERSON_ID)),
                        var().rel($author2).rel($firstName).isa(has(FIRST_NAME)),
                        var().rel($author2).rel($lastName).isa(has(LAST_NAME))
                ).get().execute();

                List<LdbcShortQuery7MessageRepliesResult> result = results.stream()
                        .sorted(comparing(by($date)).reversed().thenComparing(by($personId)))
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
                    var().rel(FRIEND, var().id(author1)).rel(FRIEND, var().id(author2)).isa(KNOWS)
            ).aggregate(ask()).execute();
        }

        private ConceptId conceptId(Answer result, Var var) {
            return result.get(var).getId();
        }
    }
}