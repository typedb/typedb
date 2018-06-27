/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package strategy;

import ai.grakn.concept.AttributeType;
import pdf.PDF;
import pick.StreamProviderInterface;


/**
 * @param <OwnerDatatype>
 */
public class AttributeStrategy<OwnerDatatype, ValueDatatype> extends TypeStrategy<AttributeType> implements HasPicker{

    private PickableCollection<AttributeOwnerTypeStrategy<OwnerDatatype>> attributeOwnerStrategies = null;
    private final StreamProviderInterface<ValueDatatype> valuePicker;
    private AttributeOwnerTypeStrategy<OwnerDatatype> attributeOwnerStrategy = null;

    public AttributeStrategy(AttributeType type,
                             PDF numInstancesPDF,
                             PickableCollection<AttributeOwnerTypeStrategy<OwnerDatatype>> attributeOwnerStrategies,
                             StreamProviderInterface<ValueDatatype> valuePicker) {
        super(type, numInstancesPDF);
        this.attributeOwnerStrategies = attributeOwnerStrategies;
        this.valuePicker = valuePicker;
    }

    public AttributeStrategy(AttributeType type,
                             PDF numInstancesPDF,
                             AttributeOwnerTypeStrategy<OwnerDatatype> attributeOwnerStrategy,
                             StreamProviderInterface<ValueDatatype> valuePicker) {
        super(type, numInstancesPDF);
        this.attributeOwnerStrategy = attributeOwnerStrategy;
        this.valuePicker = valuePicker;
    }

    public AttributeOwnerTypeStrategy<OwnerDatatype> getAttributeOwnerStrategy() {
        if (this.attributeOwnerStrategy != null) {
            return this.attributeOwnerStrategy;
        } else if (this.attributeOwnerStrategies != null) {
            return this.attributeOwnerStrategies.next();
        }
        throw new UnsupportedOperationException("AttributeStrategy must have either one owner or multiple possible owners");
    }

    @Override
    public StreamProviderInterface<ValueDatatype> getPicker() {
        return this.valuePicker;
    }
}