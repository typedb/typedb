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
import java.util.Set;

public abstract class Diagnostics {

    protected static final Logger LOG = LoggerFactory.getLogger(Diagnostics.class);

    protected static Diagnostics diagnostics = null;

    protected final Metrics metrics;

    /* separate services, kept here so that they don't get GC'd */
    protected final StatisticReporter statisticReporter;
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
        public void mayStartMonitoringService(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {}

        @Override
        public void mayStartReporting() {}

        @Override
        public void submitError(@Nullable String databaseName, Throwable error) {}

        @Override
        public void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {}

        @Override
        public void requestSuccess(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {}

        @Override
        public void incrementCurrentCount(@Nullable String databaseName, Metrics.ConnectionPeakCounts.Kind kind) {}

        @Override
        public void decrementCurrentCount(@Nullable String databaseName, Metrics.ConnectionPeakCounts.Kind kind) {}

        @Override
        public void synchronizeDatabaseDiagnostics(Set<Metrics.DatabaseDiagnostics> databaseDiagnostics) {}
    }

    public static class Core extends Diagnostics {
        static private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

        protected Core(Metrics metrics, StatisticReporter statisticReporter, MonitoringServer monitoringServer) {
            super(metrics, statisticReporter, monitoringServer);
        }

        public static synchronized void initialise(
                String deploymentID, String serverID, String distributionName, String version,
                boolean errorReportingEnabled, String errorReportingURI,
                boolean statisticsReportingEnabled, String statisticsReportingURI,
                boolean monitoringEnabled, int monitoringPort,
                Path dataDirectory,
                boolean developmentModeEnabled
        ) {
            if (diagnostics != null) {
                LOG.debug("Skipping re-initialising diagnostics");
                return;
            }

            initSentry(developmentModeEnabled, serverID, distributionName, version, errorReportingEnabled, errorReportingURI);

            Metrics metrics =
                    new Metrics(deploymentID, serverID, distributionName, version, statisticsReportingEnabled, dataDirectory);
            MonitoringServer monitoringServer = initMonitoringServer(monitoringEnabled, monitoringPort, metrics);
            StatisticReporter statisticReporter = initStatisticReporter(
                    developmentModeEnabled, deploymentID, statisticsReportingEnabled, statisticsReportingURI, metrics, dataDirectory);

            diagnostics = new Core(metrics, statisticReporter, monitoringServer);
        }

        @Nullable
        protected static MonitoringServer initMonitoringServer(boolean monitoringEnabled, int monitoringPort, Metrics metrics) {
            if (monitoringEnabled) return new MonitoringServer(metrics, monitoringPort);
            else return null;
        }

        @Nullable
        protected static StatisticReporter initStatisticReporter(
                boolean developmentModeEnabled,
                String deploymentID,
                boolean statisticsReportingEnabled,
                String statisticsReportingURI,
                Metrics metrics,
                Path dataDirectory
        ) {
            if (!developmentModeEnabled) return new StatisticReporter(
                    deploymentID, metrics, statisticsReportingEnabled, statisticsReportingURI, dataDirectory);
            else return null;
        }

        protected static void initSentry(
                boolean developmentModeEnabled,
                String serverID,
                String distributionName,
                String version,
                boolean errorReportingEnabled,
                String errorReportingURI
        ) {
            boolean sentryEnabled = !developmentModeEnabled && errorReportingEnabled;

            Sentry.init(options -> {
                options.setEnabled(sentryEnabled);
                options.setDsn(errorReportingURI);
                options.setEnableTracing(true);
                options.setSendDefaultPii(false);
                options.setRelease(releaseName(distributionName, version));
                options.setPrintUncaughtStackTrace(true);
            });
            User user = new User();
            user.setUsername(serverID);
            Sentry.setUser(user);

            // FIXME temporary heartbeat every 24 hours (https://github.com/vaticle/typedb/pull/7045)
            if (sentryEnabled) {
                scheduled.schedule(() -> {
                    Sentry.startTransaction(new TransactionContext("server", "bootup")).finish(SpanStatus.OK);
                }, 1, TimeUnit.HOURS);
                scheduled.scheduleAtFixedRate(() -> {
                    Sentry.startTransaction(new TransactionContext("server", "heartbeat")).finish(SpanStatus.OK);
                }, 25, 24, TimeUnit.HOURS);
            }
        }

        @Override
        public void mayStartMonitoringService(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware) {
            if (monitoringServer != null) monitoringServer.startServing(sslContext, middleware);
        }

        @Override
        public void mayStartReporting() {
            if (statisticReporter != null) statisticReporter.startReporting();
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
            metrics.requestSuccess(databaseName, kind);
        }

        @Override
        public void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind) {
            metrics.requestFail(databaseName, kind);
        }

        @Override
        public void incrementCurrentCount(@Nullable String databaseName, Metrics.ConnectionPeakCounts.Kind kind) {
            metrics.incrementCurrentCount(databaseName, kind);
        }

        @Override
        public void decrementCurrentCount(@Nullable String databaseName, Metrics.ConnectionPeakCounts.Kind kind) {
            metrics.decrementCurrentCount(databaseName, kind);
        }

        @Override
        public void synchronizeDatabaseDiagnostics(Set<Metrics.DatabaseDiagnostics> databaseDiagnostics
        ) {
            metrics.submitDatabaseDiagnostics(databaseDiagnostics);
        }
    }

    private static String releaseName(String distributionName, String version) {
        return distributionName + "@" + version;
    }

    public static Diagnostics get() {
        assert diagnostics != null;
        return diagnostics;
    }

    public abstract void mayStartMonitoringService(@Nullable SslContext sslContext, ChannelInboundHandlerAdapter... middleware);

    public abstract void mayStartReporting();

    public abstract void submitError(@Nullable String databaseName, Throwable error);

    public abstract void requestFail(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind);

    public abstract void requestSuccess(@Nullable String databaseName, Metrics.NetworkRequests.Kind kind);

    public abstract void incrementCurrentCount(@Nullable String databaseName, Metrics.ConnectionPeakCounts.Kind kind);

    public abstract void decrementCurrentCount(@Nullable String databaseName, Metrics.ConnectionPeakCounts.Kind kind);

    public abstract void synchronizeDatabaseDiagnostics(Set<Metrics.DatabaseDiagnostics> databaseDiagnostics);
}
