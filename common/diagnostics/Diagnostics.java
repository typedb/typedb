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

public class Diagnostics {

    private static final Logger LOG = LoggerFactory.getLogger(Diagnostics.class);

    private static Diagnostics diagnostics = null;

    private final ErrorReporter errorReporter;

    /*
     * Private singleton constructor
     */
    private Diagnostics(ErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }

    public static synchronized void initialise(boolean enable, String serverID, String distributionName, String version, String diagnosticsURI,
                                               ErrorReporter errorReporter) {
        if (diagnostics != null) {
            LOG.debug("Skipping re-initialising diagnostics");
            return;
        }
        Sentry.init(options -> {
            options.setEnabled(enable);
            options.setDsn(diagnosticsURI);
            options.setEnableTracing(true);
            options.setSendDefaultPii(false);
            options.setRelease(releaseName(distributionName, version));
        });
        User user = new User();
        user.setUsername(serverID);
        Sentry.setUser(user);
        diagnostics = new Diagnostics(errorReporter);
    }

    public static synchronized void initialiseNoop() {
        Sentry.init(options -> options.setEnabled(false));
        diagnostics = new Diagnostics(new ErrorReporter.NoopReporter());
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
}
