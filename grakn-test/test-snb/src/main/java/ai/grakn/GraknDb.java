package ai.grakn;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.IOException;
import java.util.Map;

/**
 * @author Felix Chapman
 */
public class GraknDb extends Db {

    private static GraknDbConnectionState connectionState = null;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        connectionState = new GraknDbConnectionState(properties);

        // TODO: disabling a lot because they are slow
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, GraknShortQueryHandlers.LdbcShortQuery1PersonProfileHandler.class);
//        registerOperationHandler(LdbcShortQuery2PersonPosts.class, GraknShortQueryHandlers.LdbcShortQuery2PersonPostsHandler.class);
//        registerOperationHandler(LdbcShortQuery3PersonFriends.class, GraknShortQueryHandlers.LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, GraknShortQueryHandlers.LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, GraknShortQueryHandlers.LdbcShortQuery5MessageCreatorHandler.class);
//        registerOperationHandler(LdbcShortQuery6MessageForum.class, GraknShortQueryHandlers.LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, GraknShortQueryHandlers.LdbcShortQuery7MessageRepliesHandler.class);

        registerOperationHandler(LdbcUpdate1AddPerson.class, GraknUpdateQueryHandlers.LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, GraknUpdateQueryHandlers.LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, GraknUpdateQueryHandlers.LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, GraknUpdateQueryHandlers.LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, GraknUpdateQueryHandlers.LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, GraknUpdateQueryHandlers.LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, GraknUpdateQueryHandlers.LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, GraknUpdateQueryHandlers.LdbcUpdate8AddFriendshipHandler.class);

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
