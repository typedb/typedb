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

import static ai.grakn.SNB.FORUM;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import com.google.common.collect.ImmutableSet;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import static ai.grakn.SNB.$author;
import static ai.grakn.SNB.$city;
import static ai.grakn.SNB.$comment;
import static ai.grakn.SNB.$country;
import static ai.grakn.SNB.$forum;
import static ai.grakn.SNB.$message;
import static ai.grakn.SNB.$mod;
import static ai.grakn.SNB.$original;
import static ai.grakn.SNB.$person;
import static ai.grakn.SNB.$post;
import static ai.grakn.SNB.ADMIRER;
import static ai.grakn.SNB.BIRTHDAY;
import static ai.grakn.SNB.BROWSER_USED;
import static ai.grakn.SNB.CLASS_YEAR;
import static ai.grakn.SNB.COMMENT;
import static ai.grakn.SNB.COMPANY;
import static ai.grakn.SNB.CONTAINED;
import static ai.grakn.SNB.CONTAINER;
import static ai.grakn.SNB.CONTAINER_OF;
import static ai.grakn.SNB.CONTENT;
import static ai.grakn.SNB.CREATION_DATE;
import static ai.grakn.SNB.CREATOR;
import static ai.grakn.SNB.EMAIL;
import static ai.grakn.SNB.EMPLOYEE;
import static ai.grakn.SNB.EMPLOYER;
import static ai.grakn.SNB.FIRST_NAME;
import static ai.grakn.SNB.FORUM_ID;
import static ai.grakn.SNB.FRIEND;
import static ai.grakn.SNB.GENDER;
import static ai.grakn.SNB.GROUP;
import static ai.grakn.SNB.HAS_CREATOR;
import static ai.grakn.SNB.HAS_INTEREST;
import static ai.grakn.SNB.HAS_MEMBER;
import static ai.grakn.SNB.HAS_MODERATOR;
import static ai.grakn.SNB.HAS_TAG;
import static ai.grakn.SNB.IMAGE_FILE;
import static ai.grakn.SNB.INTEREST;
import static ai.grakn.SNB.INTERESTED;
import static ai.grakn.SNB.IS_LOCATED_IN;
import static ai.grakn.SNB.JOIN_DATE;
import static ai.grakn.SNB.KNOWS;
import static ai.grakn.SNB.LANGUAGE;
import static ai.grakn.SNB.LAST_NAME;
import static ai.grakn.SNB.LENGTH;
import static ai.grakn.SNB.LIKE;
import static ai.grakn.SNB.LIKES;
import static ai.grakn.SNB.LOCATED;
import static ai.grakn.SNB.LOCATION_IP;
import static ai.grakn.SNB.MEMBER;
import static ai.grakn.SNB.MESSAGE_ID;
import static ai.grakn.SNB.MODERATED;
import static ai.grakn.SNB.MODERATOR;
import static ai.grakn.SNB.ORGANISATION_ID;
import static ai.grakn.SNB.ORIGINAL;
import static ai.grakn.SNB.PERSON;
import static ai.grakn.SNB.PERSON_ID;
import static ai.grakn.SNB.PLACE_ID;
import static ai.grakn.SNB.POST;
import static ai.grakn.SNB.PRODUCT;
import static ai.grakn.SNB.REGION;
import static ai.grakn.SNB.REPLY;
import static ai.grakn.SNB.REPLY_OF;
import static ai.grakn.SNB.SCHOOL;
import static ai.grakn.SNB.SPEAKS;
import static ai.grakn.SNB.STUDENT;
import static ai.grakn.SNB.STUDY_AT;
import static ai.grakn.SNB.TAG;
import static ai.grakn.SNB.TAGGED;
import static ai.grakn.SNB.TAG_ID;
import static ai.grakn.SNB.TITLE;
import static ai.grakn.SNB.TOPIC;
import static ai.grakn.SNB.UNIVERSITY;
import static ai.grakn.SNB.WORK_AT;
import static ai.grakn.SNB.WORK_FROM;
import static ai.grakn.SNB.fromDate;
import static ai.grakn.graql.Graql.var;

/**
 * Implementations of the LDBC SNB Update Queries
 *
 * @author sheldon, miko
 */
public class GraknUpdateQueryHandlers {

    /**
     * Update Query 1
     */
    public static class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, GraknDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                ImmutableSet.Builder<VarPattern> match = ImmutableSet.builder();
                ImmutableSet.Builder<VarPattern> insert = ImmutableSet.builder();

                match.add($city.has(PLACE_ID, operation.cityId()));

                for (Long theTag : operation.tagIds()) {
                    Var $tag = var(theTag.toString());
                    match.add($tag.isa(TAG).has(TAG_ID, theTag));
                    insert.add(var().rel(INTERESTED, $person).rel(INTEREST, $tag).isa(HAS_INTEREST));
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.studyAt()) {
                    Var $org = var(Long.toString(org.organizationId()));
                    match.add($org.isa(UNIVERSITY).has(ORGANISATION_ID, org.organizationId()));
                    insert.add(var().rel(STUDENT, $person).rel(SCHOOL, $org).isa(STUDY_AT).has(CLASS_YEAR, org.year()));
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.workAt()) {
                    Var $org = var(Long.toString(org.organizationId()));
                    match.add($org.isa(COMPANY).has(ORGANISATION_ID, org.organizationId()));
                    insert.add(var().rel(EMPLOYEE, $person).rel(EMPLOYER, $org).isa(WORK_AT).has(WORK_FROM, org.year()));
                }

                insert.add($person.isa(PERSON)
                        .has(PERSON_ID, operation.personId())
                        .has(FIRST_NAME, operation.personFirstName())
                        .has(LAST_NAME, operation.personLastName())
                        .has(BIRTHDAY, fromDate(operation.birthday()))
                        .has(CREATION_DATE, fromDate(operation.creationDate()))
                        .has(LOCATION_IP, operation.locationIp())
                        .has(BROWSER_USED, operation.browserUsed())
                        .has(GENDER, operation.gender()));

                for (String language : operation.languages()) {
                    insert.add($person.has(SPEAKS, language));
                }

                for (String theEmail : operation.emails()) {
                    insert.add($person.has(EMAIL, theEmail));
                }

                insert.add(var().rel(LOCATED, $person).rel(REGION, $city).isa(IS_LOCATED_IN));

                graph.graql().match(match.build()).insert(insert.build()).execute();
                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }

        }
    }

    /**
     * Update Query 2
     */
    public static class LdbcUpdate2AddPostLikeHandler implements OperationHandler<LdbcUpdate2AddPostLike, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                graph.graql().match(
                        $person.has(PERSON_ID, operation.personId()),
                        $message.has(MESSAGE_ID, operation.postId())
                ).insert(var()
                        .rel(ADMIRER, $person).rel(LIKE, $message).isa(LIKES)
                        .has(CREATION_DATE, fromDate(operation.creationDate()))
                ).execute();

                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }

        }
    }

    /**
     * Update Query 3
     */
    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                graph.graql().match(
                        $person.has(PERSON_ID, operation.personId()),
                        $message.has(MESSAGE_ID, operation.commentId())
                ).insert(var()
                        .rel(ADMIRER, $person).rel(LIKE, $message).isa(LIKES)
                        .has(CREATION_DATE, fromDate(operation.creationDate()))
                ).execute();

                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);

            }

        }
    }

    /**
     * Update Query 4
     */
    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate4AddForum operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                ImmutableSet.Builder<VarPattern> match = ImmutableSet.builder();
                ImmutableSet.Builder<VarPattern> insert = ImmutableSet.builder();

                match.add($mod.has(PERSON_ID, operation.moderatorPersonId()));

                for (long tag : operation.tagIds()) {
                    Var $tag = var(Long.toString(tag));
                    match.add($tag.has(TAG_ID, tag));
                    insert.add(var().rel(TAGGED, $forum).rel(TOPIC, $tag).isa(HAS_TAG));
                }

                insert.add($forum.isa(FORUM)
                        .has(FORUM_ID, operation.forumId())
                        .has(TITLE, operation.forumTitle())
                        .has(CREATION_DATE, fromDate(operation.creationDate()))
                );

                insert.add(var().rel(MODERATOR, $mod).rel(MODERATED, $forum).isa(HAS_MODERATOR));

                graph.graql().match(match.build()).insert(insert.build()).execute();
                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);

            }
        }
    }

    /**
     * Update Query 5
     */
    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                graph.graql().match(
                        $forum.has(FORUM_ID, operation.forumId()),
                        $person.has(PERSON_ID, operation.personId())
                ).insert(var()
                        .rel(MEMBER, $person).rel(GROUP, $forum).isa(HAS_MEMBER)
                        .has(JOIN_DATE, fromDate(operation.joinDate()))
                ).execute();

                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);

            }
        }
    }

    /**
     * Update Query 6
     */
    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate6AddPost operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                ImmutableSet.Builder<VarPattern> match = ImmutableSet.builder();
                ImmutableSet.Builder<VarPattern> insert = ImmutableSet.builder();

                match.add(
                        $author.has(PERSON_ID, operation.authorPersonId()),
                        $forum.has(FORUM_ID, operation.forumId()),
                        $country.has(PLACE_ID, operation.countryId())
                );

                for (long tag : operation.tagIds()) {
                    Var $tag = var(Long.toString(tag));
                    match.add($tag.has(TAG_ID, tag));
                    insert.add(var().rel(TAGGED, $post).rel(TOPIC, $tag).isa(HAS_TAG));
                }

                insert.add($post.isa(POST).has(MESSAGE_ID, operation.postId())
                        .has(LOCATION_IP, operation.locationIp())
                        .has(BROWSER_USED, operation.browserUsed())
                        .has(LENGTH, operation.length())
                        .has(CREATION_DATE, fromDate(operation.creationDate())));

                if (operation.language().length() > 0) {
                    insert.add($post.has(LANGUAGE, operation.language()));
                }
                if (operation.imageFile().length() > 0) {
                    insert.add($post.has(IMAGE_FILE, operation.imageFile()));
                } else {
                    insert.add($post.has(CONTENT, operation.content()));
                }
                insert.add(
                        var().rel(PRODUCT, $post).rel(CREATOR, $author).isa(HAS_CREATOR),
                        var().rel(LOCATED, $post).rel(REGION, $country).isa(IS_LOCATED_IN),
                        var().rel(CONTAINED, $post).rel(CONTAINER, $forum).isa(CONTAINER_OF)
                );

                graph.graql().match(match.build()).insert(insert.build()).execute();
                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }
        }

    }

    /**
     * Update Query 7
     */
    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate7AddComment operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                ImmutableSet.Builder<VarPattern> match = ImmutableSet.builder();
                ImmutableSet.Builder<VarPattern> insert = ImmutableSet.builder();

                match.add($author.has(PERSON_ID, operation.authorPersonId()));
                if (operation.replyToPostId() != -1) {
                    match.add($original.has(MESSAGE_ID, operation.replyToPostId()));
                } else {
                    match.add($original.has(MESSAGE_ID, operation.replyToCommentId()));
                }
                match.add($country.has(PLACE_ID, operation.countryId()));

                for (long tag : operation.tagIds()) {
                    Var $tag = var(Long.toString(tag));
                    match.add($tag.has(TAG_ID, tag));
                    insert.add(var().rel(TAGGED, $comment).rel(TOPIC, $tag).isa(HAS_TAG));
                }

                insert.add(
                        $comment.isa(COMMENT).has(MESSAGE_ID, operation.commentId())
                                .has(CONTENT, operation.content())
                                .has(LOCATION_IP, operation.locationIp())
                                .has(BROWSER_USED, operation.browserUsed())
                                .has(CREATION_DATE, fromDate(operation.creationDate()))
                                .has(LENGTH, operation.length()),

                        var().rel(PRODUCT, $comment).rel(CREATOR, $author).isa(HAS_CREATOR),
                        var().rel(LOCATED, $comment).rel(REGION, $country).isa(IS_LOCATED_IN),
                        var().rel(REPLY, $comment).rel(ORIGINAL, $original).isa(REPLY_OF)
                );

                graph.graql().match(match.build()).insert(insert.build()).execute();

                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }
        }

    }

    /**
     * Update Query 8
     */
    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation,
                                     GraknDbConnectionState dbConnectionState,
                                     ResultReporter reporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {
                Var $person1 = var("person1");
                Var $person2 = var("person2");

                graph.graql().match(
                        $person1.has(PERSON_ID, operation.person1Id()),
                        $person2.has(PERSON_ID, operation.person2Id())
                ).insert(var()
                        .rel(FRIEND, $person1).rel(FRIEND, $person2).isa(KNOWS)
                        .has(CREATION_DATE, fromDate(operation.creationDate()))
                ).execute();

                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }
        }
    }
}
