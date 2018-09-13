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

package strategy;

import ai.grakn.concept.Type;
import pdf.PDF;

/**
 * @param <T>
 */
public class TypeStrategy<T extends Type> implements TypeStrategyInterface {
    private final T type;
    private final String typeLabel;
//    private final P numInstancesPDF;
    private final PDF numInstancesPDF;

    public TypeStrategy(T type, PDF numInstancesPDF){
        this.type = type;
        this.numInstancesPDF = numInstancesPDF;
        // TODO Storing the label value can be avoided when TP functionality #20179 is complete
        this.typeLabel = this.type.label().getValue();
    }

    public T getType() {
        return type;
    }

    public String getTypeLabel() {
        return typeLabel;
    }

    public PDF getNumInstancesPDF() {
        return numInstancesPDF;
    }
}

