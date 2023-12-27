/*
 * Copyright (C) 2023 Vaticle
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
 */

package com.vaticle.typedb.core.server.logging;

import com.vaticle.typedb.common.collection.Pair;
import io.sentry.ISpan;
import io.sentry.ITransaction;
import io.sentry.Sentry;
import io.sentry.TracesSamplingDecision;
import io.sentry.TransactionContext;
import io.sentry.protocol.User;

import javax.annotation.Nullable;

import java.util.List;

import static com.vaticle.typedb.core.server.common.Constants.DIAGNOSTICS_REPORTING_URI;

public class Diagnostics {

    private static final double SAMPLE_RATE = 0.01;

    public static void initialise(String serverID, String distributionName, String version) {
        Sentry.init(options -> {
            options.setDsn(DIAGNOSTICS_REPORTING_URI);
            options.setTracesSampleRate(SAMPLE_RATE);
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

    public static ITransaction sampledTransaction(String name, String operation, @Nullable String description, List<Pair<String, String>> tags) {
        return Sentry.startTransaction(transactionContext(name, operation, description, tags));
    }

    public static ITransaction requiredTransaction(String name, String operation, @Nullable String description, List<Pair<String, String>> tags) {
        TransactionContext context = transactionContext(name, operation, description, tags);
        context.setSampled(true);
        return Sentry.startTransaction(context);
    }

    private static TransactionContext transactionContext(String name, String operation, @Nullable String description, List<Pair<String, String>> tags) {
        TransactionContext context = new TransactionContext(name, operation);
        if (description != null) context.setDescription(description);
        for (Pair<String, String> tag : tags) {
            context.setTag(tag.first(), tag.second());
        }
        return context;
    }
}
