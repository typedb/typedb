/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package configure;


import java.util.List;

/**
 * Absorbs a toplevel benchmark configuration file
 */

public class BenchmarkConfigurationFile {
    private String name;
    private String schema;
    private String queries;
    private List<Integer> concepts;

    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return this.name;
    }

    public void setSchema(String schemaFile) {
        this.schema = schemaFile;
    }
    public String getSchemaFile() {
        return System.getProperty("user.dir") + this.schema;
    }

    public void setQueries(String queriesYaml) {
        this.queries = queriesYaml;
    }
    public String getQueriesYamlFile() {
        return System.getProperty("user.dir") + this.queries;
    }

    public void setConcepts(List<Integer> concepts) {
        this.concepts = concepts;
    }
    public List<Integer> getConceptsToBenchmark() {
        return this.concepts;
    }

}

