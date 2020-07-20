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

package grakn.core.common.options;

public class GraknOptions {

    public static final boolean DEFAULT_INFER = true;
    public static final boolean DEFAULT_EXPLAIN = false;
    public static final int DEFAULT_BATCH_SIZE = 50;

    private Boolean infer = null;
    private Boolean explain = null;
    private Integer batchSize = null;

    public GraknOptions() {}

    public Boolean infer() {
        if (infer != null) return infer;
        else return DEFAULT_INFER;
    }

    public void infer(boolean infer) {
        this.infer = infer;
    }

    public Boolean explain() {
        return explain;
    }

    public void explain(boolean explain) {
        this.explain = explain;
    }

    public Integer batchSize() {
        return batchSize;
    }

    public void batchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
