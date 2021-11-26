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
import com.vaticle.typedb.core.logic.Rule;

import java.util.function.Function;

public class ConclusionController2 extends Controller<Rule.Conclusion, ConceptMap, ConclusionController2.ConclusionAns, ConclusionController2.ConclusionProcessor, ConclusionController2> {
    protected ConclusionController2(Driver<ConclusionController2> driver, String name, Rule.Conclusion id, ActorExecutorGroup executorService) {
        super(driver, name, id, executorService);
    }

    @Override
    protected Function<Driver<ConclusionProcessor>, ConclusionProcessor> createProcessorFunc() {
        return null;
    }

    @Override
    protected <UPS_CID, UPS_PID, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, ?, UPS_CONTROLLER>> UpstreamHandler<UPS_CID, UPS_PID, ?, UPS_CONTROLLER> getUpstreamHandler(UPS_CID id) {
        return null;
    }

    public static class ConclusionAns {}

    public class ConclusionProcessor extends Processor<ConclusionAns, ConclusionProcessor> {
        public ConclusionProcessor(Driver<ConclusionProcessor> driver, Driver<? extends Controller<?, ?,
                ConclusionAns, ConclusionProcessor, ?>> controller, String name, OutletManager outletManager) {
            super(driver, controller, name, outletManager);
        }

        @Override
        public <INLET_MANAGER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR extends Processor<INPUT, UPSTREAM_PROCESSOR>> InletManager<INLET_ID, INPUT, UPSTREAM_PROCESSOR> getInletManager(INLET_MANAGER_ID controllerId) {
            return null;
        }
    }
}
