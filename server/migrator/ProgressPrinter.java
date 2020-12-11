/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.migrator;

import java.util.Timer;
import java.util.TimerTask;

public class ProgressPrinter {

    private static final String[] ANIM = new String[] {
            "-",
            "\\",
            "|",
            "/"
    };
    private static final String STATUS_STARTING = "starting";
    private static final String STATUS_IN_PROGRESS = "in progress";
    private static final String STATUS_COMPLETED = "completed";

    private final String type;
    private final Timer timer = new Timer();

    private String status = STATUS_STARTING;
    private long current = 0;
    private long total = 0;

    private int anim = 0;
    private int lines = 0;

    public ProgressPrinter(String type) {
        this.type = type;
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                step();
            }
        };
        timer.scheduleAtFixedRate(task, 0, 100);
    }

    public void onProgress(long current, long total) {
        status = STATUS_IN_PROGRESS;
        this.current = current;
        this.total = total;
    }

    public void onCompletion() {
        status = STATUS_COMPLETED;
        step();
        timer.cancel();
    }

    private synchronized void step() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("$x isa %s,\n    has status \"%s\"", type, status));

        if (status.equals(STATUS_IN_PROGRESS)) {
            String percent;
            if (total > 0) {
                percent = String.format("%.1f", (double)current / (double)total * 100.0);
            } else {
                percent = "100";
            }
            builder.append(String.format(",\n    has progress (%s%%),\n    has count (%,d / %,d)",
                    percent, current, total));
        }

        builder.append(";");
        if (status.equals(STATUS_IN_PROGRESS)) {
            anim = (anim + 1) % ANIM.length;
            builder.append(" ").append(ANIM[anim]);
        }

        String output = builder.toString();
        System.out.println((lines > 0 ? "\033[" + lines + "F\033[J" : "") + output);

        lines = output.split("\n").length;
    }
}
