/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.core.concurrent.actor.Actor.Driver;
import com.vaticle.typedb.core.reasoner.resolution.resolver.BoundConcludableResolver;

import java.util.function.Consumer;

public class ReiterationQuery {
    public static class Request {
        private final Driver<? extends Resolver<?>> rootResolver;
        private final Consumer<Response> onResponse;

        private Request(Driver<? extends Resolver<?>> rootResolver, Consumer<Response> onResponse) {
            this.rootResolver = rootResolver;
            this.onResponse = onResponse;
        }

        public static Request create(Driver<? extends Resolver<?>> rootResolver, Consumer<Response> onResponse) {
            return new Request(rootResolver, onResponse);
        }

        public Driver<? extends Resolver<?>> sender() {
            return rootResolver;
        }

        public Consumer<Response> onResponse() {
            return onResponse;
        }

    }

    public static class Response {

        private final boolean reiterate;
        private final Driver<BoundConcludableResolver> sender;

        private Response(Driver<BoundConcludableResolver> sender, boolean reiterate) {
            this.reiterate = reiterate;
            this.sender = sender;
        }

        public static Response create(Driver<BoundConcludableResolver> driver, boolean reiterate) {
            return new Response(driver, reiterate);
        }

        public Driver<BoundConcludableResolver> sender() {
            return sender;
        }

        public boolean reiterate() {
            return reiterate;
        }
    }
}
