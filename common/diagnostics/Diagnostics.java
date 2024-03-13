/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.common.diagnostics;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslContext;
import io.sentry.Sentry;
import io.sentry.protocol.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public abstract class Diagnostics {

    protected static final Logger LOG = LoggerFactory.getLogger(Diagnostics.class);

    protected static Diagnostics diagnostics = null;

    protected final Metrics metrics;

    /* separate services, kept here so that they don't get GC'd */
    private final StatisticReporter statisticReporter;
    protected final MonitoringServer monitoringServer;

    /*
     * Protected singleton constructor
     */
    protected Diagnostics(Metrics metrics, StatisticReporter statisticReporter, MonitoringServer monitoringServer) {
        this.metrics = metrics;
        this.statisticReporter = statisticReporter;
        this.monitoringServer = monitoringServer;
    }

    public static class Noop extends Diagnostics {
        private Noop() {
            super(null, null, null);
        }

        public static synchronized void initialise() {
            Sentry.init(options -> options.setEnabled(false));
            diagnostics = new Diagnostics.Noop();
        }

        @Override
        public void mayStartServing(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {}
        @Override
        public void submitError(Throwable error) {}
        @Override
        public void requestAttempt(Metrics.NetworkRequests.Kind kind) {}
        @Override
        public void requestSuccess(Metrics.NetworkRequests.Kind kind) {}
        @Override
        public void setCurrentCount(Metrics.UsageCounters.Kind kind, long value) {}
    }

    public static class Core extends Diagnostics {
        protected Core(Metrics metrics, StatisticReporter statisticReporter, MonitoringServer monitoringServer) {
            super(metrics, statisticReporter, monitoringServer);
        }

        public static synchronized void initialise(
                String serverID, String distributionName, String version,
                boolean errorReportingEnable, String errorReportingURI,
                boolean statisticsReportingEnable, String statisticsReportingURI,
                boolean monitoringEnable, int monitoringPort
        ) {
            if (diagnostics != null) {
                LOG.debug("Skipping re-initialising diagnostics");
                return;
            }

            initSentry(serverID, distributionName, version, errorReportingEnable, errorReportingURI);

            Metrics metrics = new Metrics(serverID, distributionName, version);
            StatisticReporter statisticReporter = initStatisticReporter(statisticsReportingEnable, statisticsReportingURI, metrics);
            MonitoringServer monitoringServer = initMonitoringServer(monitoringEnable, monitoringPort, metrics);

            diagnostics = new Core(metrics, statisticReporter, monitoringServer);
        }

        @Nullable
        protected static MonitoringServer initMonitoringServer(boolean monitoringEnable, int monitoringPort, Metrics metrics) {
            if (monitoringEnable) return new MonitoringServer(metrics, monitoringPort);
            else return null;
        }

        @Nullable
        protected static StatisticReporter initStatisticReporter(boolean statisticsReportingEnable, String statisticsReportingURI, Metrics metrics) {
            if (statisticsReportingEnable) return new StatisticReporter(metrics, statisticsReportingURI);
            else return null;
        }

        protected static void initSentry(String serverID, String distributionName, String version, boolean errorReportingEnable, String errorReportingURI) {
            Sentry.init(options -> {
                options.setEnabled(errorReportingEnable);
                options.setDsn(errorReportingURI);
                options.setEnableTracing(true);
                options.setSendDefaultPii(false);
                options.setRelease(releaseName(distributionName, version));
                options.setPrintUncaughtStackTrace(true);
            });
            User user = new User();
            user.setUsername(serverID);
            Sentry.setUser(user);
        }

        @Override
        public void mayStartServing(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
            if (monitoringServer != null) monitoringServer.startServing(sslContext, middleware);
        }

        @Override
        public void submitError(Throwable error) {
            if (error instanceof TypeDBException) {
                TypeDBException typeDBException = (TypeDBException) error;
                if (isUserError(typeDBException)) {
                    metrics.registerError(typeDBException.errorMessage().code());
                    return;
                }
            }
            Sentry.captureException(error);
        }

        private boolean isUserError(TypeDBException exception) {
            return !(exception.errorMessage() instanceof ErrorMessage.Internal);
        }

        @Override
        public void requestAttempt(Metrics.NetworkRequests.Kind kind) {
            metrics.requestAttempt(kind);
        }

        @Override
        public void requestSuccess(Metrics.NetworkRequests.Kind kind) {
            metrics.requestSuccess(kind);
        }

        @Override
        public void setCurrentCount(Metrics.UsageCounters.Kind kind, long value) {
            metrics.setCurrentCount(kind, value);
        }
    }

    private static String releaseName(String distributionName, String version) {
        return distributionName + "@" + version;
    }

    public static Diagnostics get() {
        assert diagnostics != null;
        return diagnostics;
    }

    public abstract void mayStartServing(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware);

    public abstract void submitError(Throwable error);

    public abstract void requestAttempt(Metrics.NetworkRequests.Kind kind);

    public abstract void requestSuccess(Metrics.NetworkRequests.Kind kind);

    public abstract void setCurrentCount(Metrics.UsageCounters.Kind kind, long value);
}
