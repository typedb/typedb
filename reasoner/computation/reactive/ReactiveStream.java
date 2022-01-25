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

package com.vaticle.typedb.core.reasoner.computation.reactive;

public abstract class ReactiveStream<INPUT, OUTPUT> extends PublisherImpl<OUTPUT> implements Reactive.Receiver.Subscriber<INPUT>  {

    protected ReactiveStream(PacketMonitor monitor, String groupName) {
        super(monitor, groupName);
    }

    protected abstract boolean isPulling();

    protected abstract void finishPulling();

    protected abstract Manager<INPUT> providerManager();

}
