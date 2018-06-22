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
 * @param <Datatype>
 */
public class AttributeStrategy<Datatype> extends TypeStrategy<AttributeType> implements HasPicker{

    private PickableCollection<AttributeOwnerTypeStrategy> attributeOwnerStrategies = null;
    private final StreamProviderInterface valuePicker;
    private AttributeOwnerTypeStrategy attributeOwnerStrategy = null;

    public AttributeStrategy(AttributeType type,
                             PDF numInstancesPDF,
                             PickableCollection<AttributeOwnerTypeStrategy> attributeOwnerStrategies,
                             StreamProviderInterface valuePicker) {
        super(type, numInstancesPDF);
        this.attributeOwnerStrategies = attributeOwnerStrategies;
        this.valuePicker = valuePicker;
    }

    public AttributeStrategy(AttributeType type,
                             PDF numInstancesPDF,
                             AttributeOwnerTypeStrategy attributeOwnerStrategy,
                             StreamProviderInterface valuePicker) {
        super(type, numInstancesPDF);
        this.attributeOwnerStrategy = attributeOwnerStrategy;
        this.valuePicker = valuePicker;
    }

    public AttributeOwnerTypeStrategy getAttributeOwnerStrategy() {
        if (this.attributeOwnerStrategy != null) {
            return this.attributeOwnerStrategy;
        } else if (this.attributeOwnerStrategies != null) {
            return this.attributeOwnerStrategies.next();
        }
        throw new UnsupportedOperationException("AttributeStrategy must have either one owner or multiple possible owners");
    }

    @Override
    public StreamProviderInterface getPicker() {
        return this.valuePicker;
    }
}