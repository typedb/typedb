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

import ai.grakn.graql.InsertQuery;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                StringBuilder query = new StringBuilder("match ");
                StringBuilder interests = new StringBuilder();
                StringBuilder workAndStudyPlaces = new StringBuilder();

                query.append("$city has place-id " + operation.cityId() + "; ");


                for (Long tag : operation.tagIds()) {
                    query.append("$" + tag.toString() + " isa tag has tag-id " + tag + "; ");
                    interests.append("(interested: $x, interest: $" + tag.toString() + ") isa has-interest;");
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.studyAt()) {
                    query.append("$" + org.organizationId() + " isa university has organisation-id " + org.organizationId() + "; ");
                    workAndStudyPlaces.append("(student: $x, school: $" + org.organizationId() + ") isa study-at has class-year " + org.year() + "; ");
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.workAt()) {
                    query.append("$" + org.organizationId() + " isa company has organisation-id " + org.organizationId() + "; ");
                    workAndStudyPlaces.append("(employee: $x, employer: $" + org.organizationId() + ") isa work-at has work-from " + org.year() + "; ");
                }

                String baseInsertQuery = "insert " +
                        "$x isa person has person-id " + operation.personId() +
                        " has first-name '" + operation.personFirstName() + "' " +
                        "has last-name '" + operation.personLastName() + "' " +
                        "has birth-day " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.birthday().getTime()), ZoneOffset.UTC).toString() +
                        " has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() +
                        " has location-ip '" + operation.locationIp() + "' " +
                        "has browser-used '" + operation.browserUsed() + "' " +
                        "has gender '" + operation.gender() + "' ";

                query.append(baseInsertQuery);

                for (String language : operation.languages()) {
                    query.append("has speaks '" + language + "' ");
                }

                for (String email : operation.emails()) {
                    query.append("has email '" + email + "' ");
                }

                query.append("; ");

                query.append(interests);

                query.append("(located: $x, region: $city) isa is-located-in;");

                query.append(workAndStudyPlaces);

                graph.graql().<InsertQuery>parse(query.toString()).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                String query = "match " +
                        "$x has person-id " + operation.personId() + "; " +
                        "$y has message-id " + operation.postId() + "; " +
                        "insert (admirer: $x, like: $y) isa likes has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + ";";

                graph.graql().<InsertQuery>parse(query).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                String query = "match " +
                        "$x has person-id " + operation.personId() + "; " +
                        "$y has message-id " + operation.commentId() + "; " +
                        "insert (admirer: $x, like: $y) isa likes has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + ";";

                graph.graql().<InsertQuery>parse(query).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                StringBuilder query = new StringBuilder("match ");
                StringBuilder tags = new StringBuilder();

                query.append("$mod has person-id " + operation.moderatorPersonId() + "; ");


                for (long tag : operation.tagIds()) {
                    query.append("$" + tag + " has tag-id " + tag + "; ");
                    tags.append("(tagged: $forum, topic:  $" + tag + ") isa has-tag; ");
                }

                String insertQ = "insert $forum isa forum has forum-id " + operation.forumId() +
                        " has title '" + operation.forumTitle() + "' " +
                        "has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + "; ";

                query.append(insertQ);
                query.append("(moderator: $mod, moderated: $forum) isa has-moderator; ");
                query.append(tags);

                graph.graql().<InsertQuery>parse(query.toString()).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                String query = "match $forum has forum-id " + operation.forumId() + "; " +
                        " $person has person-id " + operation.personId() + "; " +
                        " insert (member: $person, group: $forum) isa has-member has join-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.joinDate().getTime()), ZoneOffset.UTC).toString() + ";";

                graph.graql().<InsertQuery>parse(query.toString()).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                StringBuilder query = new StringBuilder("match ");
                StringBuilder tags = new StringBuilder();

                query.append("$author has person-id " + operation.authorPersonId() + "; ");
                query.append("$forum has forum-id " + operation.forumId() + "; ");
                query.append("$country has place-id " + operation.countryId() + "; ");

                for (long tag : operation.tagIds()) {
                    query.append("$" + tag + " has tag-id " + tag + "; ");
                    tags.append("(tagged: $post, topic: $" + tag + ") isa has-tag; ");
                }

                String insertQ = "insert $post isa post has message-id " + operation.postId() + " " +
                        "has location-ip '" + operation.locationIp() + "' " +
                        "has browser-used '" + operation.browserUsed() + "' " +
                        "has length " + operation.length() + " " +
                        "has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + " ";

                query.append(insertQ);
                if (operation.language().length() > 0) {
                    query.append("has language '" + operation.language() + "' ");
                }
                if (operation.imageFile().length() > 0) {
                    query.append("has image-file '" + operation.imageFile() + "' ");
                } else {
                    query.append(" has content \"" + operation.content() + "\" ");
                }
                query.append(";");
                query.append("(product: $post, creator:  $author) isa has-creator; ");
                query.append("(located: $post, region: $country) isa is-located-in; ");
                query.append("(contained: $post, container: $forum) isa container-of; ");

                query.append(tags);


                graph.graql().<InsertQuery>parse(query.toString()).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                StringBuilder query = new StringBuilder("match ");
                StringBuilder tags = new StringBuilder();

                query.append("$author has person-id " + operation.authorPersonId() + "; ");
                if (operation.replyToPostId() != -1) {
                    query.append("$original has message-id " + operation.replyToPostId() + "; ");
                } else {
                    query.append("$original has message-id " + operation.replyToCommentId() + "; ");
                }
                query.append("$country has place-id " + operation.countryId() + "; ");

                for (long tag : operation.tagIds()) {
                    query.append("$" + tag + " has tag-id " + tag + "; ");
                    tags.append("(tagged: $comment, topic:  $" + tag + ") isa has-tag; ");
                }

                String insertQ = "insert $comment isa comment has message-id " + operation.commentId() + " " +
                        "has content \"" + operation.content() + "\" " +
                        "has location-ip '" + operation.locationIp() + "' " +
                        "has browser-used '" + operation.browserUsed() + "' " +
                        "has creation-date  " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + "  " +
                        "has length " + operation.length() + "; ";

                query.append(insertQ);


                query.append("(product: $comment, creator: $author) isa has-creator; ");
                query.append("(located: $comment, region: $country) isa is-located-in; ");
                query.append("(reply: $comment, original: $original) isa reply-of; ");

                query.append(tags);


                graph.graql().<InsertQuery>parse(query.toString()).execute();
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
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                String query = "match $x has person-id " + operation.person1Id() +
                        "; $y has person-id " + operation.person2Id() + ";" +
                        "insert (friend: $x, friend: $y) isa knows has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + ";";

                graph.graql().<InsertQuery>parse(query.toString()).execute();
                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }
        }
    }
}
