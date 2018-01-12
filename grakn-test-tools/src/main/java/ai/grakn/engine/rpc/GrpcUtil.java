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

package ai.grakn.engine.rpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

/**
 * @author Felix Chapman
 */
public class GrpcUtil {

    // TODO: dup code
    private static final Metadata.Key<String> MESSAGE = Metadata.Key.of("message", StringMarshaller.create());

    private static class StringMarshaller implements Metadata.AsciiMarshaller<String> {

        public static StringMarshaller create() {
            return new StringMarshaller();
        }

        @Override
        public String toAsciiString(String value) {
            return value;
        }

        @Override
        public String parseAsciiString(String serialized) {
            return serialized;
        }
    }

    public static Matcher<StatusRuntimeException> hasStatus(Status status) {
        return allOf(
                isA(StatusRuntimeException.class),
                hasProperty("status", is(status))
        );
    }

    public static Matcher<StatusRuntimeException> hasMessage(String message) {
        return allOf(
                hasStatus(Status.UNKNOWN),
                new TypeSafeMatcher<StatusRuntimeException>() {
                    @Override
                    public void describeTo(Description description) {
                        description.appendText("has message " + message);
                    }

                    @Override
                    protected boolean matchesSafely(StatusRuntimeException item) {
                        return message.equals(item.getTrailers().get(MESSAGE));
                    }
                }
        );
    }
}
