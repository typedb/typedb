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

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.Concept;

import java.util.List;

public interface Type extends Concept, Comparable<Type> {

    long getInstancesCount();

    boolean isRoot();

    void setLabel(String label);

    Label getLabel();

    boolean isAbstract();

    Type getSupertype();

    FunctionalIterator<? extends Type> getSupertypes();

    Forwardable<? extends Type, Order.Asc> getSubtypes();

    Forwardable<? extends Type, Order.Asc> getSubtypesExplicit();

    List<TypeDBException> validate();
}
