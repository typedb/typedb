/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use concept::thing::thing_manager::ThingManager;
use storage::snapshot::WritableSnapshot;

use crate::{
    pipeline::{PipelineError, StageAPI, StageIterator, WrittenRowsIterator},
    write::delete::DeleteExecutor,
};

pub struct DeleteStageExecutor<Snapshot: WritableSnapshot + 'static, PreviousStage: StageAPI<Snapshot>> {
    deleter: DeleteExecutor,
    previous: PreviousStage,
    thing_manager: Arc<ThingManager>,
    phantom: PhantomData<Snapshot>,
}

impl<Snapshot, PreviousStage> DeleteStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: WritableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    pub fn new(deleter: DeleteExecutor, previous: PreviousStage, thing_manager: Arc<ThingManager>) -> Self {
        Self { deleter, previous, thing_manager, phantom: PhantomData::default()}
    }
}

impl<Snapshot, PreviousStage> StageAPI<Snapshot> for DeleteStageExecutor<Snapshot, PreviousStage>
where
    Snapshot: WritableSnapshot + 'static,
    PreviousStage: StageAPI<Snapshot>,
{
    type OutputIterator = WrittenRowsIterator;

    fn into_iterator(self) -> Result<(Self::OutputIterator, Arc<Snapshot>), PipelineError> {
        let (previous_iterator, mut snapshot) = self.previous.into_iterator()?;
        // accumulate once, then we will operate in-place
        let mut rows = previous_iterator.collect_owned()?;

        // once the previous iterator is complete, this must be the exclusive owner of Arc's, so unwrap:
        let snapshot_ref = Arc::get_mut(&mut snapshot).unwrap();
        for index in 0..rows.len() {
            let mut row = rows.get_row_mut(index);
            self.deleter
                .execute_delete(snapshot_ref, self.thing_manager.as_ref(), &mut row)
                .map_err(|err| PipelineError::WriteError(err))?;
        }

        Ok((WrittenRowsIterator::new(rows), snapshot))
    }
}
