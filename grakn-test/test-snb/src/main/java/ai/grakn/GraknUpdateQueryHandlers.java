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

import static ai.grakn.SNB.admirer;
import static ai.grakn.SNB.birthday;
import static ai.grakn.SNB.browserUsed;
import static ai.grakn.SNB.classYear;
import static ai.grakn.SNB.comment;
import static ai.grakn.SNB.company;
import static ai.grakn.SNB.contained;
import static ai.grakn.SNB.container;
import static ai.grakn.SNB.containerOf;
import static ai.grakn.SNB.content;
import static ai.grakn.SNB.creationDate;
import static ai.grakn.SNB.creator;
import static ai.grakn.SNB.email;
import static ai.grakn.SNB.employee;
import static ai.grakn.SNB.employer;
import static ai.grakn.SNB.firstName;
import static ai.grakn.SNB.forumId;
import static ai.grakn.SNB.friend;
import static ai.grakn.SNB.fromDate;
import static ai.grakn.SNB.gender;
import static ai.grakn.SNB.hasCreator;
import static ai.grakn.SNB.hasInterest;
import static ai.grakn.SNB.hasMember;
import static ai.grakn.SNB.hasModerator;
import static ai.grakn.SNB.hasTag;
import static ai.grakn.SNB.imageFile;
import static ai.grakn.SNB.interest;
import static ai.grakn.SNB.interested;
import static ai.grakn.SNB.isLocatedIn;
import static ai.grakn.SNB.joinDate;
import static ai.grakn.SNB.knows;
import static ai.grakn.SNB.language;
import static ai.grakn.SNB.lastName;
import static ai.grakn.SNB.length;
import static ai.grakn.SNB.like;
import static ai.grakn.SNB.likes;
import static ai.grakn.SNB.located;
import static ai.grakn.SNB.locatedIn;
import static ai.grakn.SNB.locationIp;
import static ai.grakn.SNB.member;
import static ai.grakn.SNB.messageId;
import static ai.grakn.SNB.moderated;
import static ai.grakn.SNB.moderator;
import static ai.grakn.SNB.organisationId;
import static ai.grakn.SNB.original;
import static ai.grakn.SNB.person;
import static ai.grakn.SNB.personId;
import static ai.grakn.SNB.placeId;
import static ai.grakn.SNB.post;
import static ai.grakn.SNB.product;
import static ai.grakn.SNB.region;
import static ai.grakn.SNB.reply;
import static ai.grakn.SNB.replyOf;
import static ai.grakn.SNB.school;
import static ai.grakn.SNB.speaks;
import static ai.grakn.SNB.student;
import static ai.grakn.SNB.studyAt;
import static ai.grakn.SNB.tag;
import static ai.grakn.SNB.tagId;
import static ai.grakn.SNB.tagged;
import static ai.grakn.SNB.title;
import static ai.grakn.SNB.topic;
import static ai.grakn.SNB.university;
import static ai.grakn.SNB.workAt;
import static ai.grakn.SNB.workFrom;
import static ai.grakn.graql.Graql.var;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.group;

/**
 * Implementations of the LDBC SNB Update Queries
 *
 * @author sheldon, miko
 */
public class GraknUpdateQueryHandlers {

    private static final Var $person = var("person");
    private static final Var $city = var("city");
    private static final Var $message = var("message");
    private static final Var $mod = var("mod");
    private static final Var $forum = var("forum");
    private static final Var $author = var("author");
    private static final Var $country = var("country");
    private static final Var $post = var("post");
    private static final Var $original = var("original");
    private static final Var $comment = var("comment");

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

                insert.add($person.isa(person)
                        .has(personId, operation.personId())
                        .has(firstName, operation.personFirstName())
                        .has(lastName, operation.personLastName())
                        .has(birthday, fromDate(operation.birthday()))
                        .has(creationDate, fromDate(operation.creationDate()))
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

                graph.graql().match(
                        $person.has(personId, operation.personId()),
                        $message.has(messageId, operation.postId())
                ).insert(var()
                        .rel(admirer, $person).rel(like, $message).isa(likes)
                        .has(creationDate, fromDate(operation.creationDate()))
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
                        $person.has(personId, operation.personId()),
                        $message.has(messageId, operation.commentId())
                ).insert(var()
                        .rel(admirer, $person).rel(like, $message).isa(likes)
                        .has(creationDate, fromDate(operation.creationDate()))
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

                match.add($mod.has(personId, operation.moderatorPersonId()));

                for (long tag : operation.tagIds()) {
                    Var $tag = var(Long.toString(tag));
                    match.add($tag.has(tagId, tag));
                    insert.add(var().rel(tagged, $forum).rel(topic, $tag).isa(hasTag));
                }

                insert.add($forum
                        .has(forumId, operation.forumId())
                        .has(title, operation.forumTitle())
                        .has(creationDate, fromDate(operation.creationDate()))
                );

                insert.add(var().rel(moderator, $mod).rel(moderated, $forum).isa(hasModerator));

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
                        $forum.has(forumId, operation.forumId()),
                        $person.has(personId, operation.personId())
                ).insert(var()
                        .rel(member, $person).rel(group, $forum).isa(hasMember)
                        .has(joinDate, fromDate(operation.joinDate()))
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
                        $author.has(personId, operation.authorPersonId()),
                        $forum.has(forumId, operation.forumId()),
                        $country.has(placeId, operation.countryId())
                );

                for (long tag : operation.tagIds()) {
                    Var $tag = var(Long.toString(tag));
                    match.add($tag.has(tagId, tag));
                    insert.add(var().rel(tagged, $post).rel(topic, $tag).isa(hasTag));
                }

                insert.add($post.isa(post).has(messageId, operation.postId())
                        .has(locationIp, operation.locationIp())
                        .has(browserUsed, operation.browserUsed())
                        .has(length, operation.length())
                        .has(creationDate, fromDate(operation.creationDate())));

                if (operation.language().length() > 0) {
                    insert.add($post.has(language, operation.language()));
                }
                if (operation.imageFile().length() > 0) {
                    insert.add($post.has(imageFile, operation.imageFile()));
                } else {
                    insert.add($post.has(content, operation.content()));
                }
                insert.add(
                        var().rel(product, $post).rel(creator, $author).isa(hasCreator),
                        var().rel(located, $post).rel(region, $country).isa(isLocatedIn),
                        var().rel(contained, $post).rel(container, $forum).isa(containerOf)
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

                match.add($author.has(personId, operation.authorPersonId()));
                if (operation.replyToPostId() != -1) {
                    match.add($original.has(messageId, operation.replyToPostId()));
                } else {
                    match.add($original.has(messageId, operation.replyToCommentId()));
                }
                match.add($country.has(placeId, operation.countryId()));

                for (long tag : operation.tagIds()) {
                    Var $tag = var(Long.toString(tag));
                    match.add($tag.has(tagId, tag));
                    insert.add(var().rel(tagged, $comment).rel(topic, $tag).isa(hasTag));
                }

                insert.add(
                        $comment.isa(comment).has(messageId, operation.commentId())
                                .has(content, operation.content())
                                .has(locationIp, operation.locationIp())
                                .has(browserUsed, operation.browserUsed())
                                .has(creationDate, fromDate(operation.creationDate()))
                                .has(length, operation.length()),

                        var().rel(product, $comment).rel(creator, $author).isa(hasCreator),
                        var().rel(located, $comment).rel(region, $country).isa(isLocatedIn),
                        var().rel(reply, $comment).rel(original, $original).isa(replyOf)
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
                        $person1.has(personId, operation.person1Id()),
                        $person2.has(personId, operation.person2Id())
                ).insert(var()
                        .rel(friend, $person1).rel(friend, $person2).isa(knows)
                        .has(creationDate, fromDate(operation.creationDate()))
                ).execute();

                graph.commit();

                reporter.report(0, LdbcNoResult.INSTANCE, operation);
            }
        }
    }
}
