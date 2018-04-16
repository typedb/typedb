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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn;

/*-
 * #%L
 * test-snb
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.io.IOException;
import java.util.Map;

/**
 * Register the queries that are implemented in Grakn with the ldbc driver.
 *
 * @author sheldon, felix
 */
@SuppressWarnings("unused")//Used as part of SNB load test
public class GraknDb extends Db {

    private GraknDbConnectionState connectionState = null;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        connectionState = new GraknDbConnectionState(properties);

        registerOperationHandler(LdbcShortQuery1PersonProfile.class, GraknShortQueryHandlers.LdbcShortQuery1PersonProfileHandler.class);

        // TODO: This query is very slow because it does ordering within Graql
//        registerOperationHandler(LdbcShortQuery2PersonPosts.class, GraknShortQueryHandlers.LdbcShortQuery2PersonPostsHandler.class);

        // TODO: This query seems to hold up validation starting for unclear reasons
//        registerOperationHandler(LdbcShortQuery3PersonFriends.class, GraknShortQueryHandlers.LdbcShortQuery3PersonFriendsHandler.class);

        registerOperationHandler(LdbcShortQuery4MessageContent.class, GraknShortQueryHandlers.LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, GraknShortQueryHandlers.LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, GraknShortQueryHandlers.LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, GraknShortQueryHandlers.LdbcShortQuery7MessageRepliesHandler.class);

        // IMPORTANT: the other queries are dependent on these update queries having executed
        registerOperationHandler(LdbcUpdate1AddPerson.class, GraknUpdateQueryHandlers.LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, GraknUpdateQueryHandlers.LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, GraknUpdateQueryHandlers.LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, GraknUpdateQueryHandlers.LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, GraknUpdateQueryHandlers.LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, GraknUpdateQueryHandlers.LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, GraknUpdateQueryHandlers.LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, GraknUpdateQueryHandlers.LdbcUpdate8AddFriendshipHandler.class);

        // TODO: disabling because they are slow
//        registerOperationHandler(LdbcQuery1.class, GraknQueryHandlers.LdbcQuery1Handler.class);
//        registerOperationHandler(LdbcQuery2.class, GraknQueryHandlers.LdbcQuery2Handler.class);
//        registerOperationHandler(LdbcQuery8.class, GraknQueryHandlers.LdbcQuery8Handler.class);
//        registerOperationHandler(LdbcQuery13.class, GraknQueryHandlers.LdbcQuery13Handler.class);

    }

    @Override
    protected void onClose() throws IOException {
        connectionState.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }
}
