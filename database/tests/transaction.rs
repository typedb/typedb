/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use database::{
    database_manager::DatabaseManager,
    transaction::{TransactionError, TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use options::TransactionOptions;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, init_logging, TempDir};
use tokio::{
    runtime::Runtime,
    sync::{broadcast, mpsc, Notify},
    time::sleep,
};

const DB_NAME: &'static str = "test";

macro_rules! assert_ok {
    ($res:ident) => {
        assert!($res.is_ok(), "{:?}", $res.as_ref().unwrap_err());
    };
}

macro_rules! assert_transaction_timeout {
    ($error:ident) => {
        let error_str = format!("{:?}", $error);
        assert!(error_str.contains("Transaction timeout"));
    };
}

fn create_database(databases_path: &TempDir) -> Arc<Database<WALClient>> {
    let database_manager = DatabaseManager::new(databases_path).expect("Expected database manager");
    database_manager.create_database(DB_NAME).expect("Expected database creation");
    database_manager.database(DB_NAME).expect("Expected database retrieval")
}

fn open_schema(database: Arc<Database<WALClient>>) -> TransactionSchema<WALClient> {
    let open_result = TransactionSchema::open(database, TransactionOptions::default());
    assert_ok!(open_result);
    open_result.unwrap()
}

fn open_write(database: Arc<Database<WALClient>>) -> TransactionWrite<WALClient> {
    let open_result = TransactionWrite::open(database, TransactionOptions::default());
    assert_ok!(open_result);
    open_result.unwrap()
}

fn open_read(database: Arc<Database<WALClient>>) -> TransactionRead<WALClient> {
    let open_result = TransactionRead::open(database, TransactionOptions::default());
    assert_ok!(open_result);
    open_result.unwrap()
}

fn transaction_sleep_timeout() -> Duration {
    let lock_timeout_millis = TransactionOptions::default().schema_lock_acquire_timeout_millis;
    let timeout_diff_millis = 5000;
    assert!(
        lock_timeout_millis > timeout_diff_millis,
        "The transaction timeout has changed, tweak the test's sleep duration!"
    );
    Duration::from_millis(lock_timeout_millis - timeout_diff_millis)
}

////////////////////////////////
// OPEN-CLOSE-COMMIT-ROLLBACK //
////////////////////////////////

#[test]
fn open_close_schema_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_schema = open_schema(database.clone());
    tx_schema.close()
}

#[test]
fn open_rollback_schema_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_schema = open_schema(database.clone());
    tx_schema.rollback()
}

#[test]
fn open_commit_schema_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_schema = open_schema(database.clone());
    let commit_result = tx_schema.commit();
    assert_ok!(commit_result);
}

#[test]
fn open_close_write_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_write = open_write(database.clone());
    tx_write.close()
}

#[test]
fn open_rollback_write_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_write = open_write(database.clone());
    tx_write.rollback()
}

#[test]
fn open_commit_write_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_write = open_write(database.clone());
    let commit_result = tx_write.commit();
    assert_ok!(commit_result);
}

#[test]
fn open_close_read_transaction() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let mut tx_read = open_read(database.clone());
    tx_read.close()
}

/////////////////////////////
// SCHEMA TRANSACTION LOCK //
/////////////////////////////

#[test]
fn schema_transaction_blocks_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime.block_on(async move {
        let database_clone = database.clone();
        let _tx_schema = open_schema(database_clone);

        tokio::spawn(async move {
            let error = TransactionSchema::open(database, TransactionOptions::default()).unwrap_err();
            assert_transaction_timeout!(error);
        })
        .await
        .unwrap();
    });
}

#[test]
fn schema_transaction_blocks_concurrent_write_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let _tx_schema = open_schema(database.clone());
    let write_error = TransactionWrite::open(database, TransactionOptions::default()).unwrap_err();
    let error_str = format!("{write_error:?}");
    assert!(error_str.contains("Transaction timeout"));
}

#[test]
fn schema_transaction_does_not_block_concurrent_read_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let _tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_read = open_read(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_can_be_opened_after_prior_timeout_error() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction2_failed = Arc::new(Notify::new());
            let notify_transaction2_failed_clone = notify_transaction2_failed.clone();
            let notify_transaction1_done = Arc::new(Notify::new());
            let notify_transaction1_done_clone = notify_transaction1_done.clone();

            let mut tx_schema = open_schema(database_clone);
            let task1 = tokio::spawn(async move {
                notify_transaction2_failed_clone.notified().await;
                tx_schema.close();
                notify_transaction1_done.notify_one();
            });

            let task2 = tokio::spawn(async move {
                let database_clone = database.clone();
                let error = TransactionSchema::open(database_clone, TransactionOptions::default()).unwrap_err();
                assert_transaction_timeout!(error);
                notify_transaction2_failed.notify_one();
                notify_transaction1_done_clone.notified().await;
                let _tx_schema = open_schema(database);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_close_unblocks_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_schema.close();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_schema = open_schema(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_commit_unblocks_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_schema.commit().expect("Expected commit");
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_schema = open_schema(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_rollback_does_not_unblock_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_schema.rollback();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let error = TransactionSchema::open(database_clone, TransactionOptions::default()).unwrap_err();
                assert_transaction_timeout!(error);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_close_unblocks_concurrent_write_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_schema.close();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_write = open_write(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_commit_unblocks_concurrent_write_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_schema.commit().expect("Expected commit");
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_write = open_write(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn schema_transaction_rollback_does_not_unblock_concurrent_write_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_schema.rollback();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let error = TransactionWrite::open(database_clone, TransactionOptions::default()).unwrap_err();
                assert_transaction_timeout!(error);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

/////////////////////////////
// WRITE TRANSACTIONS LOCK //
/////////////////////////////

#[test]
fn write_transaction_blocks_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime.block_on(async move {
        let database_clone = database.clone();
        let notify_transaction1_ready = Arc::new(Notify::new());
        let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

        tokio::spawn(async move {
            let _tx_write = open_write(database);
            notify_transaction1_ready.notify_one();
        })
        .await
        .unwrap();

        tokio::spawn(async move {
            notify_transaction1_ready_clone.notified().await;
            let error = TransactionSchema::open(database_clone, TransactionOptions::default()).unwrap_err();
            assert_transaction_timeout!(error);
        })
        .await
        .unwrap();
    });
}

#[test]
fn write_transaction_close_unblocks_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_write = open_write(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_write.close();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_schema = open_schema(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn write_transaction_commit_unblocks_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_write = open_write(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_write.commit().expect("Expected commit");
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let _tx_schema = open_schema(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn write_transaction_rollback_does_not_unblock_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let notify_transaction1_ready = Arc::new(Notify::new());
            let notify_transaction1_ready_clone = notify_transaction1_ready.clone();

            let task1 = tokio::spawn(async move {
                let mut tx_write = open_write(database);
                notify_transaction1_ready.notify_one();
                sleep(transaction_sleep_timeout()).await;
                tx_write.rollback();
            });

            let task2 = tokio::spawn(async move {
                notify_transaction1_ready_clone.notified().await;
                let error = TransactionSchema::open(database_clone, TransactionOptions::default()).unwrap_err();
                assert_transaction_timeout!(error);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn write_transaction_does_not_block_concurrent_write_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let task1 = tokio::spawn(async move {
                let _tx_write = open_write(database);
            });

            let task2 = tokio::spawn(async move {
                let _tx_write = open_write(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

#[test]
fn write_transaction_does_not_block_concurrent_read_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let _tx_write = open_write(database.clone());
    let _tx_read = open_read(database);
}

/////////////////////////////
// READ TRANSACTIONS LOCK? //
/////////////////////////////

#[test]
fn read_transaction_does_not_block_concurrent_schema_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let _tx_read = open_read(database.clone());
    let _tx_schema = open_schema(database);
}

#[test]
fn read_transaction_does_not_block_concurrent_write_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);
    let _tx_read = open_read(database.clone());
    let _tx_write = open_write(database);
}

#[test]
fn read_transaction_does_not_block_concurrent_read_transactions() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let task1 = tokio::spawn(async move {
                let _tx_read = open_read(database);
            });

            let task2 = tokio::spawn(async move {
                let _tx_read = open_read(database_clone);
            });

            tokio::try_join!(task1, task2)
        })
        .unwrap();
}

////////////////////////////////////
// COMPLICATED TRANSACTION QUEUES //
////////////////////////////////////

#[test]
fn blocked_schema_transactions_progress_one_at_a_time() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime.block_on(async move {
        let database_clone = database.clone();
        let database_clone_2 = database.clone();
        let database_clone_3 = database.clone();
        let database_clone_4 = database.clone();
        let (sender, mut receiver2) = broadcast::channel(1);
        let mut receiver3 = sender.subscribe();
        let mut receiver4 = sender.subscribe();
        let mut receiver5 = sender.subscribe();

        const MULTIPLIER: u64 = 18; // Big multiplier for small machines
        let full_timeout_millis = TransactionOptions::default().schema_lock_acquire_timeout_millis;
        let timeout_millis = Duration::from_millis(full_timeout_millis / MULTIPLIER);

        let task2 = tokio::spawn(async move {
            receiver2.recv().await.expect("Expected receiver2");
            let tx_schema = open_schema(database_clone);
            sleep(timeout_millis).await;
            tx_schema.close();
            Instant::now()
        });

        let task3 = tokio::spawn(async move {
            receiver3.recv().await.expect("Expected receiver3");
            let tx_schema = open_schema(database_clone_2);
            sleep(timeout_millis).await;
            tx_schema.close();
            Instant::now()
        });

        let task4 = tokio::spawn(async move {
            receiver4.recv().await.expect("Expected receiver4");
            let tx_schema = open_schema(database_clone_3);
            sleep(timeout_millis).await;
            tx_schema.close();
            Instant::now()
        });

        let task5 = tokio::spawn(async move {
            receiver5.recv().await.expect("Expected receiver5");
            let tx_schema = open_schema(database_clone_4);
            sleep(timeout_millis).await;
            tx_schema.close();
            Instant::now()
        });

        let task_main = tokio::spawn(async move {
            let mut tx_write = open_write(database);
            sender.send(()).expect("Expected send");;
            sleep(timeout_millis).await;
            tx_write.close();
        });

        let (_, result2, result3, result4, result5) = tokio::join!(task_main, task2, task3, task4, task5);
        let mut opened = vec![
            result2.expect("expected result2"),
            result3.expect("expected result3"),
            result4.expect("expected result4"),
            result5.expect("expected result5"),
        ];
        opened.sort();
        assert!(opened.get(1).unwrap().duration_since(*opened.get(0).unwrap()) >= timeout_millis);
        assert!(opened.get(2).unwrap().duration_since(*opened.get(1).unwrap()) >= timeout_millis);
        assert!(opened.get(3).unwrap().duration_since(*opened.get(2).unwrap()) >= timeout_millis);
    });
}

#[test]
fn blocked_write_transactions_progress_together() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime.block_on(async move {
        let database_clone = database.clone();
        let database_clone_2 = database.clone();
        let database_clone_3 = database.clone();
        let database_clone_4 = database.clone();
        let (sender, mut receiver2) = broadcast::channel(1);
        let mut receiver3 = sender.subscribe();
        let mut receiver4 = sender.subscribe();
        let mut receiver5 = sender.subscribe();

        const MULTIPLIER: u64 = 18; // Big multiplier for small machines
        let full_timeout_millis = TransactionOptions::default().schema_lock_acquire_timeout_millis;
        let timeout_millis = Duration::from_millis(full_timeout_millis / MULTIPLIER);

        let task2 = tokio::spawn(async move {
            receiver2.recv().await.expect("Expected receiver2");
            let tx_write = open_write(database_clone);
            sleep(timeout_millis).await;
            tx_write.close();
            Instant::now()
        });

        let task3 = tokio::spawn(async move {
            receiver3.recv().await.expect("Expected receiver3");
            let tx_write = open_write(database_clone_2);
            sleep(timeout_millis).await;
            tx_write.close();
            Instant::now()
        });

        let task4 = tokio::spawn(async move {
            receiver4.recv().await.expect("Expected receiver4");
            let tx_write = open_write(database_clone_3);
            sleep(timeout_millis).await;
            tx_write.close();
            Instant::now()
        });

        let task5 = tokio::spawn(async move {
            receiver5.recv().await.expect("Expected receiver5");
            let tx_write = open_write(database_clone_4);
            sleep(timeout_millis).await;
            tx_write.close();
            Instant::now()
        });

        let task_main = tokio::spawn(async move {
            let mut tx_schema = open_schema(database);
            sender.send(()).expect("Expected send");;
            sleep(timeout_millis).await;
            tx_schema.close();
        });

        let (_, result2, result3, result4, result5) = tokio::join!(task_main, task2, task3, task4, task5);
        let mut opened = vec![
            result2.expect("expected result2"),
            result3.expect("expected result3"),
            result4.expect("expected result4"),
            result5.expect("expected result5"),
        ];
        opened.sort();
        assert!(opened.get(1).unwrap().duration_since(*opened.get(0).unwrap()) < timeout_millis);
        assert!(opened.get(2).unwrap().duration_since(*opened.get(1).unwrap()) < timeout_millis);
        assert!(opened.get(3).unwrap().duration_since(*opened.get(2).unwrap()) < timeout_millis);
    });
}

#[test]
fn blocked_schema_and_write_transactions_can_progress_in_different_orders() {
    init_logging();
    let databases_path = create_tmp_dir();
    let database = create_database(&databases_path);

    let runtime = Runtime::new().expect("Expected runtime");
    runtime
        .block_on(async move {
            let database_clone = database.clone();
            let database_clone_2 = database.clone();
            let database_clone_3 = database.clone();
            let database_clone_4 = database.clone();
            let (sender, mut receiver2) = broadcast::channel(1);
            let mut receiver3 = sender.subscribe();
            let mut receiver4 = sender.subscribe();
            let mut receiver5 = sender.subscribe();

            const MULTIPLIER: u64 = 18; // Big multiplier for small machines
            let full_timeout_millis = TransactionOptions::default().schema_lock_acquire_timeout_millis;
            let timeout_millis = Duration::from_millis(full_timeout_millis / MULTIPLIER);

            let task_write = tokio::spawn(async move {
                receiver2.recv().await.expect("Expected receiver2");
                let tx_write = open_write(database_clone);
                sleep(timeout_millis).await;
                tx_write.close();
                Instant::now()
            });

            let task_schema = tokio::spawn(async move {
                receiver3.recv().await.expect("Expected receiver3");
                let tx_schema = open_schema(database_clone_2);
                sleep(timeout_millis).await;
                tx_schema.close();
                Instant::now()
            });

            let task_write_2 = tokio::spawn(async move {
                receiver4.recv().await.expect("Expected receiver4");
                let tx_write = open_write(database_clone_3);
                sleep(timeout_millis).await;
                tx_write.close();
                Instant::now()
            });

            let task_schema_2 = tokio::spawn(async move {
                receiver5.recv().await.expect("Expected receiver5");
                let tx_schema = open_schema(database_clone_4);
                sleep(timeout_millis).await;
                tx_schema.close();
                Instant::now()
            });

            let task_main = tokio::spawn(async move {
                let mut tx_schema = open_schema(database);
                sender.send(()).expect("Expected send");
                sleep(timeout_millis).await;
                tx_schema.close();
            });

            // We don't care what's the order and the specific timings.
            // All tasks should eventually progress before the timeout
            tokio::try_join!(task_main, task_write, task_schema, task_write_2, task_schema_2)
        })
        .unwrap();
}
