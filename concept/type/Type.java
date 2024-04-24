/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.concept.Concept;

import java.util.List;

public interface Type extends Concept, Concept.Readable, Comparable<Type> {

    long getInstancesCount();

    boolean isRoot();

    void setLabel(String label);

    Label getLabel();

    boolean isAbstract();

    Type getSupertype();

    Forwardable<? extends Type, Order.Asc> getSupertypes();

    Forwardable<? extends Type, Order.Asc> getSubtypes();

    Forwardable<? extends Type, Order.Asc> getSubtypes(Transitivity transitivity);

    List<TypeDBException> exceptions();
}
