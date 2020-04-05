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
 *
 */

package hypergraph.test.behaviour.config;

import hypergraph.Hypergraph;
import io.cucumber.java.DataTableType;
import io.cucumber.java.ParameterType;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class Parameters {

    public enum RootLabel {
        ENTITY("entity"),
        ATTRIBUTE("attribute"),
        RELATION("relation");

        private final String label;

        RootLabel(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }

        public static RootLabel of(String label) {
            for (RootLabel t : RootLabel.values()) {
                if (t.label.equals(label)) {
                    return t;
                }
            }
            return null;
        }
    }
    @ParameterType("true|false")
    public Boolean bool(String bool) {
        return Boolean.parseBoolean(bool);
    }

    @ParameterType("[0-9]+")
    public Integer number(String number) {
        return Integer.parseInt(number);
    }

    @ParameterType("entity|attribute|relation")
    public RootLabel root_label(String type){
        return RootLabel.of(type);
    }

    @ParameterType("[a-zA-Z0-9-_]+")
    public String type_label(String typeLabel){
        return typeLabel;
    }

    @ParameterType("read|write")
    public Hypergraph.Transaction.Type transaction_type(String type){
        return Hypergraph.Transaction.Type.of(type);
    }

    @DataTableType
    public List<Hypergraph.Transaction.Type> transaction_types(List<String> values) {
        List<Hypergraph.Transaction.Type> typeList = new ArrayList<>();
        for (String value : values) {
            Hypergraph.Transaction.Type type = Hypergraph.Transaction.Type.of(value);
            assertNotNull(type);
            typeList.add(type);
        }

        return typeList;
    }
}
