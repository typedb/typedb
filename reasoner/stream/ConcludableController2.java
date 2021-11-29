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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.stream.ConclusionController2.ConclusionProcessor;

import javax.annotation.Nullable;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class ConcludableController2 extends Controller<Concludable, ConceptMap, ConcludableController2.ConcludableAns, ConcludableController2.ConcludableProcessor, ConcludableController2> {
    private final UpstreamHandler<Rule.Conclusion, ConceptMap, ConclusionController2.ConclusionAns, ConclusionController2, ConclusionProcessor> conclusionHandler;

    protected ConcludableController2(Driver<ConcludableController2> driver, String name, Concludable id, ActorExecutorGroup executorService) {
        super(driver, name, id, executorService);
        this.conclusionHandler = new ConclusionHandler();
    }

    @Override
    protected Function<Driver<ConcludableProcessor>, ConcludableProcessor> createProcessorFunc() {
        return null;
    }

    @Override
    protected <UPS_CID, UPS_PID, UPS_CONTROLLER extends Controller<UPS_CID, UPS_PID, ?, UPS_PROCESSOR,
            UPS_CONTROLLER>, UPS_PROCESSOR extends Processor<?, UPS_PROCESSOR>> UpstreamHandler<UPS_CID, UPS_PID, ?,
            UPS_CONTROLLER, UPS_PROCESSOR> getUpstreamHandler(UPS_CID id, @Nullable Driver<UPS_CONTROLLER> controller) {
        if (id instanceof Rule.Conclusion) {
            return (UpstreamHandler<UPS_CID, UPS_PID, ?, UPS_CONTROLLER, UPS_PROCESSOR>) conclusionHandler;  // TODO: Using instanceof requires that we do a casting. Ideally we would avoid this.
        } else {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    class ConclusionHandler extends ConcludableController2.UpstreamHandler<Rule.Conclusion, ConceptMap, ConclusionController2.ConclusionAns, ConclusionController2, ConclusionProcessor> {

        @Override
        protected Driver<ConclusionController2> getControllerForId(Rule.Conclusion id) {
            return null;  // TODO: Go to the registry and get it
        }
    }

    public static class ConcludableAns {}  // TODO: Some wrapper of answers from concludable resolution

    public static class ConcludableProcessor extends Processor<ConcludableAns, ConcludableProcessor> {
        public ConcludableProcessor(Driver<ConcludableProcessor> driver,
                                    Driver<ConcludableController2> controller, String name,
                                    OutletManager outletManager) {
            super(driver, controller, name, outletManager);
        }

        @Override
        public <INLET_MANAGER_ID, INLET_ID, INPUT, UPSTREAM_PROCESSOR extends Processor<INPUT, UPSTREAM_PROCESSOR>> InletManager<INLET_ID, INPUT, UPSTREAM_PROCESSOR> getInletManager(INLET_MANAGER_ID controllerId) {
            return null;
        }
    }
}
