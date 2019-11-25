/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.hadoop.formats;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 */
public class FormatTools {

    public static Class getBaseOutputFormatClass(Job job) {
        try {
            if (LazyOutputFormat.class.isAssignableFrom(job.getOutputFormatClass())) {
                Class<OutputFormat> baseClass = (Class<OutputFormat>)
                    job.getConfiguration().getClass(LazyOutputFormat.OUTPUT_FORMAT, null);
                return (null == baseClass) ? job.getOutputFormatClass() : baseClass;
            }
            return job.getOutputFormatClass();
        } catch (Exception e) {
            return null;
        }
    }
}
