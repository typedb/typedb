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

package ai.grakn.shell;import org.eclipse.jetty.websocket.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;

public class SessionMock implements Session {

    private final Consumer<String> onMessage;

    public SessionMock(Consumer<String> onMessage) {
        this.onMessage = onMessage;
    }

    public SessionMock(Session other, BiConsumer<Session, String> onMessage) {
        this.onMessage = string -> onMessage.accept(other, string);
    }

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
        return new RemoteEndpoint() {
            @Override
            public void sendBytes(ByteBuffer byteBuffer) throws IOException {

            }

            @Override
            public Future<Void> sendBytesByFuture(ByteBuffer byteBuffer) {
                return null;
            }

            @Override
            public void sendBytes(ByteBuffer byteBuffer, WriteCallback writeCallback) {

            }

            @Override
            public void sendPartialBytes(ByteBuffer byteBuffer, boolean b) throws IOException {

            }

            @Override
            public void sendPartialString(String s, boolean b) throws IOException {

            }

            @Override
            public void sendPing(ByteBuffer byteBuffer) throws IOException {

            }

            @Override
            public void sendPong(ByteBuffer byteBuffer) throws IOException {

            }

            @Override
            public void sendString(String s) throws IOException {
                sendStringByFuture(s);
            }

            @Override
            public Future<Void> sendStringByFuture(String s) {
                assertFalse("Message too long", s.length() > 2000);

                onMessage.accept(s);
                return null;
            }

            @Override
            public void sendString(String s, WriteCallback writeCallback) {

            }

            @Override
            public BatchMode getBatchMode() {
                return null;
            }

            @Override
            public void setBatchMode(BatchMode batchMode) {

            }

            @Override
            public void flush() throws IOException {

            }
        };
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
