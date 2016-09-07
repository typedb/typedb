/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.postprocessing;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.AbstractMindmapsGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConceptFixer {
    private static final Logger LOG = LoggerFactory.getLogger(ConceptFixer.class);
    private static final int MAX_RETRY = 10;

    public static void checkCasting(MindmapsGraph graph, String castingId){
        boolean notDone = true;
        int retry = 0;

        while(notDone) {
            try {

                if (((AbstractMindmapsGraph)graph).fixDuplicateCasting(castingId)) {
                    graph.commit();
                }
                notDone = false;
            } catch (Exception e) {
                LOG.error(ErrorMessage.POSTPROCESSING_ERROR.getMessage("casting", e.getMessage()), e);
                if(retry ++ > MAX_RETRY){
                    LOG.error(ErrorMessage.UNABLE_TO_ANALYSE_CONCEPT.getMessage(castingId, e.getMessage()), e);
                    notDone = false;
                } else {
                    performRetry(retry);
                }
            }
        }
    }

    private static int performRetry(int retry){
        retry ++;
        double seed = 1.0 + (Math.random() * 5.0);
        double waitTime = (retry * 2.0)  + seed;
        LOG.error(ErrorMessage.BACK_OFF_RETRY.getMessage(waitTime));

        try {
            Thread.sleep((long) Math.ceil(waitTime * 1000));
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        return retry;
    }
}
