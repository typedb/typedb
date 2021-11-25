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

package com.vaticle.typedb.core.reasoner.stream;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;

public class ConclusionController extends Controller<CONTROLLER_ID, ConceptMap, PROCESSOR, ConceptMap, ConclusionController> {

    protected ConclusionController(Driver<ConclusionController> driver, String name, ActorExecutorGroup executorService) {
        super(driver, name, executorService);
    }

    @Override
    protected Processor.InletManager<ConceptMap> createInletManager(ConceptMap id) {
        return null;
    }

    @Override
    protected Processor.OutletManager<ConceptMap> createOutletManager(ConceptMap id) {
        return null;
    }

    @Override
    protected Processor.Operation<ConceptMap, ConceptMap> operation(ConceptMap id) {
        return null;
    }
}
