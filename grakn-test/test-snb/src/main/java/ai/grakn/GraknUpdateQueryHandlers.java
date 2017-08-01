package ai.grakn;

import ai.grakn.graql.InsertQuery;
import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;


/**
 * Created by miko on 10/04/2017.
 */
public class GraknUpdateQueryHandlers {

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
