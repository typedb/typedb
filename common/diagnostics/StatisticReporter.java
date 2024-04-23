/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.vaticle.typedb.core.server.common.Constants.DISABLED_REPORTING_FILE_NAME;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class StatisticReporter {
    // Modify calculateInitialDelay() if you change this value!
    protected static final int REPORT_INTERVAL_MINUTES = 60;

    protected static final Logger LOG = LoggerFactory.getLogger(StatisticReporter.class);
    private final String deploymentID;
    private final Metrics metrics;
    private final String reportingURI;
    private final Path dataDirectory;

    private ScheduledFuture<?> pushScheduledTask;
    private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

    public StatisticReporter(String deploymentID, Metrics metrics, boolean statisticsReportingEnable, String reportingURI, Path dataDirectory) {
        this.deploymentID = deploymentID;
        this.metrics = metrics;
        this.reportingURI = reportingURI;
        this.dataDirectory = dataDirectory;

        if (statisticsReportingEnable) {
            deleteDisabledReportingFileIfExists();
            scheduleReporting();
        }
        else {
            reportOnceIfNeeded();
        }
    }

    private void report() {
        try {
            HttpsURLConnection conn = (HttpsURLConnection) (new URL(reportingURI)).openConnection();

            conn.setRequestMethod("POST");

            conn.setRequestProperty("Charset", "utf-8");
            conn.setRequestProperty("Connection", "close");
            conn.setRequestProperty("Content-Type", "application/json");

            conn.setDoOutput(true);
            conn.getOutputStream().write(metrics.formatJSON(true).getBytes(StandardCharsets.UTF_8));

            conn.connect();

            conn.getInputStream().readAllBytes();

            metrics.takeCountsSnapshot();
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) LOG.trace("Failed to push metrics to {}: ", reportingURI, e);
            // do nothing
        }
    }

    private void scheduleReporting() {
        pushScheduledTask = scheduled.scheduleAtFixedRate(this::report, calculateInitialDelay(), REPORT_INTERVAL_MINUTES, MINUTES);
    }

    private void reportOnceIfNeeded() {
        try {
            Path disabledReportingFile = dataDirectory.resolve(DISABLED_REPORTING_FILE_NAME);
            if (!disabledReportingFile.toFile().exists()) {
                report();
                Files.writeString(disabledReportingFile, LocalDateTime.now().toString());
            }
        } catch (IOException e) {
            LOG.debug("Failed to create or read disabled reporting file: ", e);
        }
    }

    private void deleteDisabledReportingFileIfExists() {
        try {
            Files.deleteIfExists(dataDirectory.resolve(DISABLED_REPORTING_FILE_NAME));
        } catch (IOException e) {
            LOG.debug("Failed to delete disabled reporting file: ", e);
        }
    }

    private long calculateInitialDelay() {
        int currentMinute = LocalDateTime.now().getMinute();
        int scheduledMinute = deploymentID.hashCode() % REPORT_INTERVAL_MINUTES;

        if (currentMinute > scheduledMinute) {
            return 60 - currentMinute + scheduledMinute;
        } else {
            return scheduledMinute - currentMinute;
        }
    }
}
