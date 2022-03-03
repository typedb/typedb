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

package com.vaticle.typedb.core.reasoner.computation.actor;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor extends Actor<Monitor> {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    private boolean terminated;

    public Monitor(Driver<Monitor> driver, String name) {
        super(driver, name);
    }

    public <R> void registerPath(Reactive.Receiver<R> receiver, Reactive.Provider<R> provider) {
        // This can terminate paths by either connecting to a provider which is a terminus or connecting to a provider we already know about (a join)
    }

    public <R> void registerTerminus(Reactive.Provider<R> provider) {

    }

    public <R> void createAnswer(Reactive.Provider<R> provider) {

    }

    public <R> void createAnswer(int numCreated, Reactive.Provider<R> provider) {

    }

    public <R> void consumeAnswer(Reactive.Receiver<R> receiver) {

    }

    @Override
    protected void exception(Throwable e) {

    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated.", cause);
        this.terminated = true;
    }

    public static class MonitorRef {

        private final Driver<Monitor> monitor;

        public MonitorRef(Driver<Monitor> monitor) {
            this.monitor = monitor;
        }

        public <R> void registerPath(Reactive.Receiver<R> receiver, Reactive.Provider<R> provider) {
            monitor.execute(actor -> actor.registerPath(receiver, provider));
        }

        public <R> void registerTerminus(Reactive.Provider<R> provider) {
            monitor.execute(actor -> actor.registerTerminus(provider));
        }

        public <R> void createAnswer(Reactive.Provider<R> provider) {
            monitor.execute(actor -> actor.createAnswer(provider));
        }

        public <R> void createAnswer(int numCreated, Reactive.Provider<R> provider) {
            monitor.execute(actor -> actor.createAnswer(numCreated, provider));
        }

        public <R> void consumeAnswer(Reactive.Receiver<R> receiver) {
            monitor.execute(actor -> actor.consumeAnswer(receiver));
        }

        public void syncAndReportPathFork(int numForks, Reactive forker) {
        }

        public void syncAndReportPathJoin(Reactive joiner) {
        }

        public void reportPathJoin(Reactive joiner) {
        }

    }
}
