/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.diagnostics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.HOURS;

public class StatisticReporter {
    protected static final Logger LOG = LoggerFactory.getLogger(StatisticReporter.class);

    private final String reportingURI;
    private final Metrics metrics;

    private ScheduledFuture<?> pushScheduledTask;
    private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

    public StatisticReporter(Metrics metrics, String reportingURI) {
        this.metrics = metrics;
        this.reportingURI = reportingURI;
        push();
    }

    private void push() {
        try {
            HttpsURLConnection conn = (HttpsURLConnection)(new URL(reportingURI)).openConnection();

            conn.setRequestMethod("POST");

            conn.setRequestProperty("Charset", "utf-8");
            conn.setRequestProperty("Connection", "close");
            conn.setRequestProperty("Content-Type", "application/json");

            conn.setDoOutput(true);
            conn.getOutputStream().write(metrics.formatJSON().getBytes(StandardCharsets.UTF_8));

            conn.connect();

            conn.getInputStream().readAllBytes();
        } catch (Exception e) {
            if (LOG.isTraceEnabled()) LOG.trace("Failed to push metrics to {}:", reportingURI, e);
            // do nothing
        } finally {
            pushScheduledTask = scheduled.schedule(this::push, 1, HOURS);
        }
    }
}
