pub mod transaction_util {
    use concept::thing::thing_manager::ThingManager;
    use concept::type_::type_manager::TypeManager;
    use database::transaction::{DataCommitError, SchemaCommitError, TransactionRead, TransactionSchema, TransactionWrite};
    use database::Database;
    use function::function_manager::FunctionManager;
    use lending_iterator::LendingIterator;
    use options::TransactionOptions;
    use query::query_manager::QueryManager;
    use std::sync::Arc;
    use storage::durability_client::WALClient;
    use storage::snapshot::{SchemaSnapshot, WriteSnapshot};

    #[derive(Debug)]
    pub struct TransactionUtil {
        database: Arc<Database<WALClient>>,
    }

    impl TransactionUtil {
        pub fn new(database: Arc<Database<WALClient>>) -> Self {
            TransactionUtil { database }
        }

        pub fn schema_transaction<T>(
            &self,
            fn_: impl Fn(
                QueryManager,
                &mut SchemaSnapshot<WALClient>,
                &TypeManager,
                &ThingManager,
                &FunctionManager,
            ) -> T
        ) -> Result<T, SchemaCommitError> {
            let TransactionSchema {
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                database,
                transaction_options,
            } = TransactionSchema::open(self.database.clone(), TransactionOptions::default());
            let mut snapshot: SchemaSnapshot<WALClient> = Arc::into_inner(snapshot).unwrap();
            let result = fn_(QueryManager::new(), &mut snapshot, &type_manager, &thing_manager, &function_manager);
            let tx = TransactionSchema::from(snapshot, type_manager, thing_manager, function_manager, database, transaction_options);
            tx.commit().map(|u| result)
        }

        pub fn read_transaction<T>(&self, fn_: impl Fn(TransactionRead<WALClient>) -> T) -> T {
            let tx: TransactionRead<WALClient> = TransactionRead::open(self.database.clone(), TransactionOptions::default());
            fn_(tx)
        }

        pub fn write_transaction<T>(
            &self,
            fn_: impl Fn(
                Arc<WriteSnapshot<WALClient>>,
                Arc<TypeManager>,
                Arc<ThingManager>,
                Arc<FunctionManager>,
                Arc<Database<WALClient>>,
                TransactionOptions
            ) -> (
                T,
                Arc<WriteSnapshot<WALClient>>
            )
        ) -> Result<T, DataCommitError> {
            let TransactionWrite {
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                database,
                transaction_options,
            } = TransactionWrite::open(self.database.clone(), TransactionOptions::default());
            let (rows, snapshot) = fn_(
                snapshot,
                type_manager.clone(),
                thing_manager.clone(),
                function_manager.clone(),
                database.clone(),
                transaction_options
            );
            let tx = TransactionWrite::from(
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                database,
                TransactionOptions::default()
            );
            tx.commit().map(|()| rows)
        }
    }
}

pub mod query_util {
    use crate::util::answer_util::collect_answer;
    use answer::variable_value::VariableValue;
    use concept::thing::thing_manager::ThingManager;
    use concept::type_::type_manager::TypeManager;
    use database::transaction::TransactionRead;
    use executor::{
        pipeline::stage::{ExecutionContext, StageIterator},
        ExecutionInterrupt,
    };
    use function::function_manager::FunctionManager;
    use query::query_manager::QueryManager;
    use std::collections::HashMap;
    use std::sync::Arc;
    use storage::durability_client::WALClient;
    use storage::snapshot::WriteSnapshot;
    use typeql::query::Pipeline;

    pub fn execute_read_pipeline(
        tx: TransactionRead<WALClient>, pipeline: &Pipeline
    ) -> (TransactionRead<WALClient>, Result<Vec<HashMap<String, VariableValue<'static>>>, ()>) {
        let prepared_pipeline = QueryManager {}.prepare_read_pipeline(
            tx.snapshot.clone(),
            &tx.type_manager,
            tx.thing_manager.clone(),
            &tx.function_manager,
            &pipeline,
        )
            .unwrap();

        let named_outputs = prepared_pipeline.rows_positions().unwrap().clone();

        let result_as_batch = match prepared_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()) {
            Ok((iterator, _)) => iterator.collect_owned(),
            Err((err, _)) => {
                return (tx, Err(()));
            }
        };

        match result_as_batch {
            Ok(batch) => (tx, Ok(collect_answer(batch, named_outputs))),
            Err(typedb_source) => (tx, Err(())),
        }
    }

    pub fn execute_write_pipeline(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: &FunctionManager,
        pipeline: &Pipeline
    ) -> (
        Result<Vec<HashMap<String, VariableValue<'static>>>, ()>,
        Arc<WriteSnapshot<WALClient>>
    ){
        let prepared_pipeline = QueryManager::new()
            .prepare_write_pipeline(
                snapshot,
                type_manager,
                thing_manager,
                function_manager,
                pipeline,
            )
            .unwrap();

        let named_outputs = prepared_pipeline.rows_positions().unwrap().clone();

        let (result_as_batch, snapshot) =
            match prepared_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()) {
                Ok((iterator, ExecutionContext { snapshot, .. })) => (iterator.collect_owned(), snapshot),
                Err((err, ExecutionContext { snapshot, .. })) => {
                    return (Err(()), snapshot);
                }
            };

        match result_as_batch {
            Ok(batch) => (Ok(collect_answer(batch, named_outputs)), snapshot),
            Err(typedb_source) => (
                Err(()),
                snapshot,
            ),
        }
    }
}

pub mod answer_util {
    use answer::variable_value::VariableValue;
    use compiler::VariablePosition;
    use executor::batch::Batch;
    use lending_iterator::LendingIterator;
    use std::collections::HashMap;
    use database::transaction::TransactionRead;
    use storage::durability_client::WALClient;

    pub fn collect_answer(
        batch: Batch, selected_outputs: HashMap<String, VariablePosition>,
    ) -> Vec<HashMap<String, VariableValue<'static>>> {
        batch.into_iterator_mut()
            .map_static(move |row| {
                let answer_map: HashMap<String, VariableValue<'static>> = selected_outputs
                    .iter()
                    .map(|(v, p)| (v.clone().to_owned(), row.get(*p).clone().into_owned()))
                    .collect::<HashMap<_, _>>();
                answer_map
            })
            .collect::<Vec<HashMap<String, VariableValue<'static>>>>()
    }

    pub fn get_string(tx: &TransactionRead<WALClient>, row: &HashMap<String, VariableValue>, var: &str) -> String {
        let var_ = row.get(var).unwrap();
        let attr = var_.as_thing().as_attribute();
        let attr_ref = attr.as_reference();
        let val = attr_ref.get_value(&*tx.snapshot, &tx.thing_manager)
            .unwrap().unwrap_string().to_string();
        val
    }
}