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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static ai.grakn.SNB.birthday;
import static ai.grakn.SNB.browserUsed;
import static ai.grakn.SNB.classYear;
import static ai.grakn.SNB.company;
import static ai.grakn.SNB.creationDate;
import static ai.grakn.SNB.email;
import static ai.grakn.SNB.employee;
import static ai.grakn.SNB.employer;
import static ai.grakn.SNB.firstName;
import static ai.grakn.SNB.gender;
import static ai.grakn.SNB.hasInterest;
import static ai.grakn.SNB.interest;
import static ai.grakn.SNB.interested;
import static ai.grakn.SNB.lastName;
import static ai.grakn.SNB.located;
import static ai.grakn.SNB.locatedIn;
import static ai.grakn.SNB.locationIp;
import static ai.grakn.SNB.organisationId;
import static ai.grakn.SNB.person;
import static ai.grakn.SNB.personId;
import static ai.grakn.SNB.placeId;
import static ai.grakn.SNB.region;
import static ai.grakn.SNB.school;
import static ai.grakn.SNB.speaks;
import static ai.grakn.SNB.student;
import static ai.grakn.SNB.studyAt;
import static ai.grakn.SNB.tag;
import static ai.grakn.SNB.tagId;
import static ai.grakn.SNB.university;
import static ai.grakn.SNB.workAt;
import static ai.grakn.SNB.workFrom;
import static ai.grakn.graql.Graql.var;

/**
 * Implementations of the LDBC SNB Update Queries
 *
 * @author sheldon, miko
 */
public class GraknUpdateQueryHandlers {

    private static final Var $person = var("person");
    private static final Var $city = var("city");

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

                match.add($city.has(placeId, operation.cityId()));

                for (Long theTag : operation.tagIds()) {
                    Var $tag = var(theTag.toString());
                    match.add($tag.isa(tag).has(tagId, theTag));
                    insert.add(var().rel(interested, $person).rel(interest, $tag).isa(hasInterest));
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.studyAt()) {
                    Var $org = var(Long.toString(org.organizationId()));
                    match.add($org.isa(university).has(organisationId, org.organizationId()));
                    insert.add(var().rel(student, $person).rel(school, $org).isa(studyAt).has(classYear, org.year()));
                }

                for (LdbcUpdate1AddPerson.Organization org : operation.workAt()) {
                    Var $org = var(Long.toString(org.organizationId()));
                    match.add($org.isa(company).has(organisationId, org.organizationId()));
                    insert.add(var().rel(employee, $person).rel(employer, $org).isa(workAt).has(workFrom, org.year()));
                }

                LocalDateTime theBirthday = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(operation.birthday().getTime()), ZoneOffset.UTC);

                LocalDateTime theCreationDate = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC);

                insert.add($person.isa(person)
                        .has(personId, operation.personId())
                        .has(firstName, operation.personFirstName())
                        .has(lastName, operation.personLastName())
                        .has(birthday, theBirthday)
                        .has(creationDate, theCreationDate)
                        .has(locationIp, operation.locationIp())
                        .has(browserUsed, operation.browserUsed())
                        .has(gender, operation.gender()));

                for (String language : operation.languages()) {
                    insert.add($person.has(speaks, language));
                }

                for (String theEmail : operation.emails()) {
                    insert.add($person.has(email, theEmail));
                }

                insert.add(var().rel(located, $person).rel(region, $city).isa(locatedIn));

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
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

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
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

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
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                String query = "match $forum has forum-id " + operation.forumId() + "; " +
                        " $person has person-id " + operation.personId() + "; " +
                        " insert (member: $person, group: $forum) isa has-member has join-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.joinDate().getTime()), ZoneOffset.UTC).toString() + ";";

                graph.graql().<InsertQuery>parse(query).execute();
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
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

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
            try (GraknTx graph = session.open(GraknTxType.WRITE)) {

                String query = "match $x has person-id " + operation.person1Id() +
                        "; $y has person-id " + operation.person2Id() + ";" +
                        "insert (friend: $x, friend: $y) isa knows has creation-date " + LocalDateTime.ofInstant(Instant.ofEpochMilli(operation.creationDate().getTime()), ZoneOffset.UTC).toString() + ";";

                graph.graql().<InsertQuery>parse(query).execute();
                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }
        }
    }
}
