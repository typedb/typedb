/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.shell;

import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.stream.Collectors;

@WebSocket
public class RemoteShell {

    private String currentGraphName=null;

    @OnWebSocketConnect
    public void onConnect(Session user) throws Exception {
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) {
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        String response = null;

        if(message.startsWith("graphName=")) setGraphName(message);
        else response=parseQuery(message);

        try {
            user.getRemote().sendString(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setGraphName(String message){
        currentGraphName=message.substring(10);
    }

    private String parseQuery(String message) {
        String response=null;
        if(currentGraphName==null) response="SUPER ERRROOORRR!!";
        try {
            QueryParser parser = QueryParser.create(GraphFactory.getInstance().getGraph(currentGraphName).getTransaction());

            //make this generic parseQuery instead of a matchquery
            response= parser.parseMatchQuery(message)
                    .resultsString()
                    .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }


}
