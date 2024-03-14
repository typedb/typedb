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
