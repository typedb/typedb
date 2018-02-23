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

package ai.grakn.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Objects;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

/**
 * @author Felix Chapman
 */
public class GrpcTestUtil {

    public static Matcher<StatusRuntimeException> hasStatus(Status status) {

        Matcher<Status> hasCode = hasProperty("code", is(status.getCode()));
        Matcher<Status> statusMatcher;

        String description = status.getDescription();

        if (description == null) {
            statusMatcher = hasCode;
        } else {
            Matcher<Status> hasDescription = hasProperty("description", is(description));
            statusMatcher = allOf(hasCode, hasDescription);
        }

        return allOf(isA(StatusRuntimeException.class), hasProperty("status", statusMatcher));
    }

    public static <T> Matcher<StatusRuntimeException> hasMetadata(Metadata.Key<T> key, T value) {
        return new TypeSafeMatcher<StatusRuntimeException>() {
            @Override
            protected boolean matchesSafely(StatusRuntimeException item) {
                return Objects.equals(item.getTrailers().get(key), value);
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(String.format("has metadata %s=%s", key.name(), value));
            }
        };
    }
}
