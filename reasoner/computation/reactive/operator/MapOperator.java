/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.computation.reactive.operator;

import com.vaticle.typedb.common.collection.Collections;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Provider.Publisher;

import java.util.function.Function;

public class MapOperator<INPUT, OUTPUT, RECEIVER> extends Operator.TransformerImpl<INPUT, OUTPUT, Publisher<INPUT>, RECEIVER> {

    private final Function<INPUT, OUTPUT> mappingFunc;

    public MapOperator(Function<INPUT, OUTPUT> mappingFunc) {
        this.mappingFunc = mappingFunc;
    }

    @Override
    public Transformed<OUTPUT, Publisher<INPUT>> accept(Publisher<INPUT> provider, INPUT packet) {
        // TODO: Here and elsewhere the provider argument is unused
        return Transformed.create(Collections.set(mappingFunc.apply(packet)));
    }

}
