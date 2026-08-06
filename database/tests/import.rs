/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs, path::Path, sync::Arc};

use database::{
    DatabaseDeleteError,
    database::DatabaseCreateError,
    database_manager::{DatabaseManager, ImportOwnership},
};
use diagnostics::diagnostics_manager::DiagnosticsManager;
use options::{DatabaseCleanupStrategy, byte_size::ByteSize};
use test_utils::{TempDir, create_tmp_dir, init_logging};

fn manager(data_dir: &TempDir, import_ownership: ImportOwnership) -> Arc<DatabaseManager> {
    let diagnostics = Arc::new(DiagnosticsManager::new_disabled());
    DatabaseManager::new(
        data_dir.as_ref(),
        diagnostics,
        ByteSize::mb(64),
        ByteSize::mb(64),
        import_ownership,
        DatabaseCleanupStrategy::Disabled,
    )
    .expect("DatabaseManager::new")
}

fn import_path(data_dir: &TempDir, name: &str) -> std::path::PathBuf {
    data_dir.as_ref().join("_import").join(name)
}

fn copy_dir(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let entry_target = target.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &entry_target);
        } else {
            fs::copy(entry.path(), &entry_target).unwrap();
        }
    }
}

#[test]
fn import_lifecycle_publishes_and_is_idempotent() {
    init_logging();
    let data_dir = create_tmp_dir("import_lifecycle");
    let dbm = manager(&data_dir, ImportOwnership::Shared);

    let staging = dbm.prepare_imported_database("typedb".to_string()).expect("prepare");
    assert!(matches!(
        *dbm.finalise_imported_database("typedb").unwrap_err(),
        DatabaseCreateError::ImportedDatabaseInUse { .. }
    ));
    drop(staging);
    dbm.finalise_imported_database("typedb").expect("finalise");
    assert!(dbm.database("typedb").is_some());
    assert!(!import_path(&data_dir, "typedb").exists());
    assert!(matches!(
        *dbm.finalise_imported_database("typedb").unwrap_err(),
        DatabaseCreateError::IsNotBeingImported { .. }
    ));
}

#[test]
fn cancelled_import_frees_the_name_for_create_and_reimport() {
    init_logging();
    let data_dir = create_tmp_dir("import_discard");
    let dbm = manager(&data_dir, ImportOwnership::Shared);

    let staging = dbm.prepare_imported_database("typedb".to_string()).expect("prepare");
    assert!(dbm.discard_imported_database("typedb").is_err());
    drop(staging);
    dbm.discard_imported_database("typedb").expect("cancel");
    assert!(!import_path(&data_dir, "typedb").exists());

    drop(dbm.prepare_imported_database("typedb".to_string()).expect("reimport after cancel"));
    dbm.discard_imported_database("typedb").expect("cancel again");
    dbm.put_database("typedb").expect("create after cancel");
}

#[test]
fn prepare_replaces_unheld_leftover_but_rejects_held_staging() {
    init_logging();
    let data_dir = create_tmp_dir("import_prepare_fresh");
    let dbm = manager(&data_dir, ImportOwnership::Exclusive);

    let staging = dbm.prepare_imported_database("fresh".to_string()).expect("first prepare");
    assert!(matches!(
        *dbm.prepare_imported_database("fresh".to_string()).unwrap_err(),
        DatabaseCreateError::ImportedDatabaseInUse { .. }
    ));
    assert!(matches!(*dbm.put_database("fresh").unwrap_err(), DatabaseCreateError::IsBeingImported { .. }));
    drop(staging);
    dbm.prepare_imported_database("fresh".to_string()).expect("prepare must replace the unheld leftover");
}

#[test]
fn prepare_rejects_existing_database_and_invalid_names() {
    init_logging();
    let data_dir = create_tmp_dir("import_prepare_rejections");
    let dbm = manager(&data_dir, ImportOwnership::Exclusive);

    dbm.put_database("existing").expect("create");
    assert!(matches!(
        *dbm.prepare_imported_database("existing".to_string()).unwrap_err(),
        DatabaseCreateError::AlreadyExists { .. }
    ));
    assert!(dbm.prepare_imported_database("_internal".to_string()).is_err());
    assert!(dbm.prepare_imported_database("not a name".to_string()).is_err());
}

#[test]
fn cancel_of_an_invalid_name_touches_nothing() {
    init_logging();
    let data_dir = create_tmp_dir("import_discard_invalid_names");
    let dbm = manager(&data_dir, ImportOwnership::Exclusive);

    dbm.prepare_imported_database("staged".to_string()).expect("prepare");
    for name in [".", "..", "../escaped", "_internal", "not a name"] {
        assert!(matches!(
            dbm.discard_imported_database(name),
            Err(DatabaseDeleteError::DatabaseIsNotBeingImported { .. })
        ));
    }
    assert!(dbm.import_directory().is_dir());
    assert!(dbm.imported_database("staged").is_some());
    dbm.discard_imported_database("staged").expect("cancel");
}

#[test]
fn dead_leftovers_are_replaced_by_create_prepare_and_cancel() {
    init_logging();
    let data_dir = create_tmp_dir("import_leftovers");
    let dbm = manager(&data_dir, ImportOwnership::Shared);
    drop(dbm.prepare_imported_database("seed".to_string()).expect("prepare seed"));

    // A live (registered) import blocks creation.
    let staging = dbm.prepare_imported_database("blocked".to_string()).expect("prepare");
    assert!(matches!(*dbm.put_database("blocked").unwrap_err(), DatabaseCreateError::IsBeingImported { .. }));
    drop(staging);
    dbm.discard_imported_database("blocked").expect("cancel");

    // An unregistered on-disk leftover is the trace of an import this server could not reopen.
    // Under Resume it keeps the name reserved for create — only the import operations that decide
    // the name's fate (a fresh prepare, a cancel) replace or remove it.
    fs::create_dir_all(import_path(&data_dir, "created-over")).unwrap();
    assert!(matches!(*dbm.put_database("created-over").unwrap_err(), DatabaseCreateError::IsBeingImported { .. }));
    assert!(import_path(&data_dir, "created-over").exists());

    fs::create_dir_all(import_path(&data_dir, "prepared-over")).unwrap();
    drop(dbm.prepare_imported_database("prepared-over".to_string()).expect("prepare must replace the leftover"));
    dbm.discard_imported_database("prepared-over").expect("cancel");

    fs::create_dir_all(import_path(&data_dir, "cancelled-over")).unwrap();
    assert!(dbm.discard_imported_database("cancelled-over").is_err());
    assert!(!import_path(&data_dir, "cancelled-over").exists(), "cancel must remove the dead leftover");
    dbm.put_database("cancelled-over").expect("create after the cancel released the name");
}

#[test]
fn exclusive_ownership_leftovers_are_healed_by_create() {
    init_logging();
    let data_dir = create_tmp_dir("import_leftovers_discard");
    let dbm = manager(&data_dir, ImportOwnership::Exclusive);

    // A standalone server has no peers to stay consistent with: a dead leftover is junk and
    // creation replaces it.
    fs::create_dir_all(import_path(&data_dir, "created-over")).unwrap();
    dbm.put_database("created-over").expect("create must replace the dead leftover");
    assert!(!import_path(&data_dir, "created-over").exists());
}

#[test]
fn exclusive_ownership_wipes_everything_at_startup() {
    init_logging();
    let data_dir = create_tmp_dir("import_discard");
    {
        let dbm = manager(&data_dir, ImportOwnership::Shared);
        drop(dbm.prepare_imported_database("staging".to_string()).expect("prepare"));
        fs::create_dir_all(import_path(&data_dir, "_cache-junk")).unwrap();
        fs::write(import_path(&data_dir, "stray-file"), b"junk").unwrap();
    }
    let dbm = manager(&data_dir, ImportOwnership::Exclusive);
    assert!(dbm.imported_database_names().is_empty());
    assert!(fs::read_dir(data_dir.as_ref().join("_import")).unwrap().next().is_none());
    dbm.put_database("staging").expect("create after discard");
}

#[test]
fn resume_recovery_restores_staging_and_discards_junk() {
    init_logging();
    let data_dir = create_tmp_dir("import_resume");
    {
        let dbm = manager(&data_dir, ImportOwnership::Shared);
        drop(dbm.prepare_imported_database("healthy".to_string()).expect("prepare"));
        fs::create_dir_all(import_path(&data_dir, "_cache-junk")).unwrap();
        fs::create_dir_all(import_path(&data_dir, "corrupt")).unwrap();
        fs::write(import_path(&data_dir, "stray-file"), b"junk").unwrap();
    }
    let dbm = manager(&data_dir, ImportOwnership::Shared);
    assert_eq!(dbm.imported_database_names(), vec!["healthy".to_string()]);
    assert!(!import_path(&data_dir, "_cache-junk").exists());
    assert!(!import_path(&data_dir, "stray-file").exists());

    // The unopenable staging is kept: it reserves the name against creation — surviving further
    // restarts — until a cancel or a fresh prepare of the import concludes it.
    assert!(import_path(&data_dir, "corrupt").exists());
    assert!(matches!(*dbm.put_database("corrupt").unwrap_err(), DatabaseCreateError::IsBeingImported { .. }));
    drop(dbm);
    let dbm = manager(&data_dir, ImportOwnership::Shared);
    assert!(matches!(*dbm.put_database("corrupt").unwrap_err(), DatabaseCreateError::IsBeingImported { .. }));
    assert!(dbm.discard_imported_database("corrupt").is_err());
    assert!(!import_path(&data_dir, "corrupt").exists());
    dbm.put_database("corrupt").expect("create after the cancel released the name");
}

#[test]
fn recovered_staging_can_be_finalised_or_cancelled() {
    init_logging();
    let data_dir = create_tmp_dir("import_resume_conclude");
    {
        let dbm = manager(&data_dir, ImportOwnership::Shared);
        drop(dbm.prepare_imported_database("published".to_string()).expect("prepare"));
        drop(dbm.prepare_imported_database("dropped".to_string()).expect("prepare"));
    }
    let dbm = manager(&data_dir, ImportOwnership::Shared);
    dbm.finalise_imported_database("published").expect("finalise recovered staging");
    assert!(dbm.database("published").is_some());
    dbm.discard_imported_database("dropped").expect("cancel recovered staging");
    assert!(!import_path(&data_dir, "dropped").exists());
}

#[test]
fn staged_leftover_of_a_published_database_is_discarded_on_recovery() {
    init_logging();
    let data_dir = create_tmp_dir("import_finalise_replay");
    {
        let dbm = manager(&data_dir, ImportOwnership::Shared);
        drop(dbm.prepare_imported_database("typedb".to_string()).expect("prepare"));
    }
    // Simulate a crash between publishing a finalised database and persisting the removal of its
    // staging copy: both are on disk at the next startup.
    copy_dir(&import_path(&data_dir, "typedb"), &data_dir.as_ref().join("typedb"));
    let dbm = manager(&data_dir, ImportOwnership::Shared);
    assert!(dbm.database("typedb").is_some(), "the published database must survive");
    assert_eq!(dbm.imported_database_names(), Vec::<String>::new());
    assert!(!import_path(&data_dir, "typedb").exists(), "the staging copy must be discarded on recovery");
    assert!(matches!(
        *dbm.finalise_imported_database("typedb").unwrap_err(),
        DatabaseCreateError::IsNotBeingImported { .. }
    ));
    dbm.put_database("typedb").expect("a served database with a healed leftover is an ordinary database");
    assert!(dbm.database("typedb").is_some());
}

#[test]
fn staging_databases_are_hidden_until_finalised() {
    init_logging();
    let data_dir = create_tmp_dir("import_hidden");
    let dbm = manager(&data_dir, ImportOwnership::Shared);

    let staging = dbm.prepare_imported_database("typedb".to_string()).expect("prepare");
    assert!(dbm.database("typedb").is_none());
    assert!(dbm.database_unrestricted("typedb").is_none());
    assert!(dbm.database_names().is_empty());
    assert!(dbm.imported_database("typedb").is_some());

    drop(staging);
    dbm.finalise_imported_database("typedb").expect("finalise");
    assert!(dbm.database("typedb").is_some());
    assert_eq!(dbm.database_names(), vec!["typedb".to_string()]);
    assert!(dbm.imported_database("typedb").is_none());
    assert!(dbm.imported_database_names().is_empty());
}

#[test]
fn recovered_staging_still_hides_and_blocks_its_name() {
    init_logging();
    let data_dir = create_tmp_dir("import_recovered_blocks");
    {
        let dbm = manager(&data_dir, ImportOwnership::Shared);
        drop(dbm.prepare_imported_database("typedb".to_string()).expect("prepare"));
    }
    let dbm = manager(&data_dir, ImportOwnership::Shared);
    assert!(dbm.database("typedb").is_none());
    assert!(dbm.database_names().is_empty());
    assert!(matches!(*dbm.put_database("typedb").unwrap_err(), DatabaseCreateError::IsBeingImported { .. }));
}

#[test]
fn concurrent_imports_of_different_names_are_independent() {
    init_logging();
    let data_dir = create_tmp_dir("import_concurrent");
    let dbm = manager(&data_dir, ImportOwnership::Shared);

    let staging_kept = dbm.prepare_imported_database("kept".to_string()).expect("prepare kept");
    drop(dbm.prepare_imported_database("published".to_string()).expect("prepare published"));
    drop(dbm.prepare_imported_database("cancelled".to_string()).expect("prepare cancelled"));
    let mut names = dbm.imported_database_names();
    names.sort();
    assert_eq!(names, vec!["cancelled".to_string(), "kept".to_string(), "published".to_string()]);

    dbm.finalise_imported_database("published").expect("finalise");
    dbm.discard_imported_database("cancelled").expect("cancel");
    assert_eq!(dbm.imported_database_names(), vec!["kept".to_string()]);
    assert!(dbm.database("published").is_some());
    drop(staging_kept);
    dbm.finalise_imported_database("kept").expect("finalise kept");
}

#[test]
fn delete_ignores_in_progress_imports_and_published_imports_are_ordinary_databases() {
    init_logging();
    let data_dir = create_tmp_dir("import_delete_interplay");
    let dbm = manager(&data_dir, ImportOwnership::Shared);

    // Deleting a name whose import is in progress touches nothing: the staging database is hidden.
    let staging = dbm.prepare_imported_database("typedb".to_string()).expect("prepare");
    assert!(dbm.delete_database("typedb").is_err());
    assert!(dbm.imported_database("typedb").is_some());
    drop(staging);
    dbm.finalise_imported_database("typedb").expect("finalise");

    // A published import is an ordinary database: deletable, recreatable, re-importable.
    dbm.delete_database("typedb").expect("delete");
    assert!(dbm.database("typedb").is_none());
    dbm.put_database("typedb").expect("recreate");
    dbm.delete_database("typedb").expect("delete again");
    drop(dbm.prepare_imported_database("typedb".to_string()).expect("reimport"));
    dbm.finalise_imported_database("typedb").expect("finalise again");
    assert!(dbm.database("typedb").is_some());
}
