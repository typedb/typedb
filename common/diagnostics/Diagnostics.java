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

import io.sentry.Sentry;
import io.sentry.protocol.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Diagnostics {

    // FIXME inject
    public static final String DIAGNOSTICS_REPORTING_URI = "https://3d710295c75c81492e57e1997d9e01e1@o4506315929812992.ingest.sentry.io/4506316048629760";
    public static final String METRICS_REPORTING_URI = "https://localhost:1500/";

    private static final Logger LOG = LoggerFactory.getLogger(Diagnostics.class);

    private static Diagnostics diagnostics = null;

    final Metrics metrics;
    private final ErrorReporter errorReporter;
    private final StatisticReporter statisticReporter;
    private final MonitoringEndpoint monitoringEndpoint;

    /*
     * Private singleton constructor
     */
    protected Diagnostics(Metrics metrics, ErrorReporter errorReporter, StatisticReporter statisticReporter, MonitoringEndpoint monitoringEndpoint) {
        this.metrics = metrics;
        this.errorReporter = errorReporter;
        this.statisticReporter = statisticReporter;
        this.monitoringEndpoint = monitoringEndpoint;
    }

    public static class Noop extends Diagnostics {
        private Noop() {
            super(null, new ErrorReporter.NoopReporter(), new StatisticReporter.NoopReporter(), null);
        }

        public static synchronized void initialise() {
            Sentry.init(options -> options.setEnabled(false));
            diagnostics = new Diagnostics.Noop();
        }

    }

    public static class Core extends Diagnostics {
        private Core(Metrics metrics, ErrorReporter errorReporter, StatisticReporter statisticReporter, MonitoringEndpoint monitoringEndpoint) {
            super(metrics, errorReporter, statisticReporter, monitoringEndpoint);
        }

        public static synchronized void initialise(
                String serverID, String distributionName, String version,
                boolean errorReportingEnable, boolean statisticsReportingEnable,
                boolean monitoringEnable, int monitoringPort
        ) {
            if (diagnostics != null) {
                LOG.debug("Skipping re-initialising diagnostics");
                return;
            }

            initSentry(serverID, distributionName, version, errorReportingEnable);

            CoreStatisticReporter statisticReporter;
            if (statisticsReportingEnable) statisticReporter = new CoreStatisticReporter(METRICS_REPORTING_URI);
            else statisticReporter = null;

            MonitoringEndpoint monitoringEndpoint;
            if (monitoringEnable) monitoringEndpoint = new CoreMonitoringEndpoint(monitoringPort);
            else monitoringEndpoint = null;

            diagnostics = new Core(
                    new Metrics(serverID, distributionName, version),
                    new CoreErrorReporter(),
                    statisticReporter,
                    monitoringEndpoint
            );

            if (statisticReporter != null) statisticReporter.push();
        }
    }

    protected static void initSentry(String serverID, String distributionName, String version, boolean errorReportingEnable) {
        Sentry.init(options -> {
            options.setEnabled(errorReportingEnable);
            options.setDsn(DIAGNOSTICS_REPORTING_URI);
            options.setEnableTracing(true);
            options.setSendDefaultPii(false);
            options.setRelease(releaseName(distributionName, version));
        });
        User user = new User();
        user.setUsername(serverID);
        Sentry.setUser(user);
    }

    private static String releaseName(String distributionName, String version) {
        return distributionName + "@" + version;
    }

    public static Diagnostics get() {
        assert diagnostics != null;
        return diagnostics;
    }

    public void submitError(Throwable error) {
        this.errorReporter.reportError(error);
    }

    public void requestAttempt(Metrics.NetworkRequests.Kind kind) {
        metrics.requestAttempt(kind);
    }

    public void requestSuccess(Metrics.NetworkRequests.Kind kind) {
        metrics.requestSuccess(kind);
    }

    public void setGauge(Metrics.UsageStatistics.Kind kind, long value) {
        metrics.setGauge(kind, value);
    }
}
