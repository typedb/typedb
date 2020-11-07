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

package grakn.core.server.rpc.util;

import grakn.core.common.parameters.Options;
import grakn.protocol.OptionsProto;

import java.util.function.Supplier;

public class RequestReader {

    public static <T extends Options<?, ?>> T getOptions(Supplier<T> optionsConstructor, OptionsProto.Options requestOptions) {
        final T options = optionsConstructor.get();
        if (requestOptions.getInferOptCase().equals(OptionsProto.Options.InferOptCase.INFER)) {
            options.infer(requestOptions.getInfer());
        }
        if (requestOptions.getExplainOptCase().equals(OptionsProto.Options.ExplainOptCase.EXPLAIN)) {
            options.explain(requestOptions.getExplain());
        }
        if (requestOptions.getBatchSizeOptCase().equals(OptionsProto.Options.BatchSizeOptCase.BATCH_SIZE)) {
            options.batchSize(requestOptions.getBatchSize());
        }

        return options;
    }
}
