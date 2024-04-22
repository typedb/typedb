/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslContext;
import io.sentry.Sentry;
import io.sentry.SpanStatus;
import io.sentry.TransactionContext;
import io.sentry.protocol.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.nio.file.Path;

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
        public void submitError(@Nullable String databaseName, Throwable error) {}
        @Override
        public void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {}
        @Override
        public void requestSuccess(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {}
        @Override
        public void incrementCurrentCount(String databaseName, Metrics.CurrentCounts.Kind kind) {}
        @Override
        public void decrementCurrentCount(String databaseName, Metrics.CurrentCounts.Kind kind) {}
    }

    public static class Core extends Diagnostics {
        static private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

        protected Core(Metrics metrics, StatisticReporter statisticReporter, MonitoringServer monitoringServer) {
            super(metrics, statisticReporter, monitoringServer);
        }

        public static synchronized void initialise(
                String deploymentID, String serverID, String distributionName, String version,
                boolean errorReportingEnable, String errorReportingURI,
                boolean statisticsReportingEnable, String statisticsReportingURI,
                boolean monitoringEnable, int monitoringPort,
                Path dataDirectory
        ) {
            if (diagnostics != null) {
                LOG.debug("Skipping re-initialising diagnostics");
                return;
            }

            initSentry(serverID, distributionName, version, errorReportingEnable, errorReportingURI);

            Metrics metrics = new Metrics(deploymentID, serverID, distributionName, version, statisticsReportingEnable, dataDirectory);
            StatisticReporter statisticReporter = initStatisticReporter(deploymentID, statisticsReportingEnable, statisticsReportingURI, metrics, dataDirectory);
            MonitoringServer monitoringServer = initMonitoringServer(monitoringEnable, monitoringPort, metrics);

            diagnostics = new Core(metrics, statisticReporter, monitoringServer);
        }

        @Nullable
        protected static MonitoringServer initMonitoringServer(boolean monitoringEnable, int monitoringPort, Metrics metrics) {
            if (monitoringEnable) return new MonitoringServer(metrics, monitoringPort);
            else return null;
        }

        protected static StatisticReporter initStatisticReporter(
                String deploymentID, boolean statisticsReportingEnable, String statisticsReportingURI, Metrics metrics, Path dataDirectory
        ) {
            return new StatisticReporter(deploymentID, metrics, statisticsReportingEnable, statisticsReportingURI, dataDirectory);
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

            // FIXME temporary heartbeat every 24 hours
            if (errorReportingEnable) {
                scheduled.schedule(() -> {
                    Sentry.startTransaction(new TransactionContext("server", "bootup")).finish(SpanStatus.OK);
                }, 1, TimeUnit.HOURS);
                scheduled.scheduleAtFixedRate(() -> {
                    Sentry.startTransaction(new TransactionContext("server", "heartbeat")).finish(SpanStatus.OK);
                }, 25, 24, TimeUnit.HOURS);
            }
        }

        @Override
        public void mayStartServing(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
            if (monitoringServer != null) monitoringServer.startServing(sslContext, middleware);
        }

        @Override
        public void submitError(@Nullable String databaseName, Throwable error) {
            if (error instanceof TypeDBException) {
                TypeDBException typeDBException = (TypeDBException) error;
                if (isUserError(typeDBException)) {
                    metrics.registerError(databaseName, typeDBException.errorMessage().code());
                    return;
                }
            }
            Sentry.captureException(error);
        }

        private boolean isUserError(TypeDBException exception) {
            return !(exception.errorMessage() instanceof ErrorMessage.Internal);
        }

        @Override
        public void requestSuccess(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {
            metrics.requestSuccess(null, databaseName, kind);
        }

        @Override
        public void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {
            metrics.requestFail(null, databaseName, kind);
        }

        @Override
        public void incrementCurrentCount(String databaseName, Metrics.CurrentCounts.Kind kind) {
            metrics.incrementCurrentCount(databaseName, kind);
        }
        @Override
        public void decrementCurrentCount(String databaseName, Metrics.CurrentCounts.Kind kind) {
            metrics.decrementCurrentCount(databaseName, kind);
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

    public abstract void submitError(@Nullable String databaseName, Throwable error);

    public abstract void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind);

    public abstract void requestSuccess(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind);

    public abstract void incrementCurrentCount(String databaseName, Metrics.CurrentCounts.Kind kind);

    public abstract void decrementCurrentCount(String databaseName, Metrics.CurrentCounts.Kind kind);
}
