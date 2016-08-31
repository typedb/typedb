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

import org.eclipse.jetty.websocket.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SessionMock implements Session {
    @Override
    public void close() {

    }

    @Override
    public void close(CloseStatus closeStatus) {

    }

    @Override
    public void close(int i, String s) {

    }

    @Override
    public void disconnect() throws IOException {

    }

    @Override
    public long getIdleTimeout() {
        return 0;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public WebSocketPolicy getPolicy() {
        return null;
    }

    @Override
    public String getProtocolVersion() {
        return null;
    }

    @Override
    public RemoteEndpoint getRemote() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public UpgradeRequest getUpgradeRequest() {
        return null;
    }

    @Override
    public UpgradeResponse getUpgradeResponse() {
        return null;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public void setIdleTimeout(long l) {

    }

    @Override
    public SuspendToken suspend() {
        return null;
    }
}
