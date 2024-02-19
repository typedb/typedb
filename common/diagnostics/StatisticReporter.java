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

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.HOURS;

public class StatisticReporter {
    // FIXME this should either go away with either the metrics push endpoint providing a trusted cert,
    // or this should be initialized with correct certs
    private SSLContext sslContext;

    private final String reportingURI;
    private final Metrics metrics;

    private ScheduledFuture<?> pushScheduledTask;
    private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

    public StatisticReporter(Metrics metrics, String reportingURI) {
        this.metrics = metrics;
        this.reportingURI = reportingURI;

        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[] {
                    new X509TrustManager() {
                        @Override public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        @Override public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                        @Override public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    }
            }, new java.security.SecureRandom());
            push();
        } catch (Exception ignored) {
            sslContext = null;
        }
    }

    private void push() {
        try {
            HttpsURLConnection conn = (HttpsURLConnection)(new URL(reportingURI)).openConnection();

            conn.setSSLSocketFactory(sslContext.getSocketFactory());

            conn.setRequestMethod("POST");

            conn.setRequestProperty("Charset", "utf-8");
            conn.setRequestProperty("Connection", "close");
            conn.setRequestProperty("Content-Type", "application/json");

            conn.setDoOutput(true);
            conn.getOutputStream().write(metrics.formatJSON().getBytes(StandardCharsets.UTF_8));

            conn.connect();

            conn.getInputStream().readAllBytes();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pushScheduledTask = scheduled.schedule(this::push, 1, HOURS);
        }
    }
}
