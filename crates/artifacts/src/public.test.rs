//! Tests for choosing the public store and for the in-memory store. The R2 and
//! Blob impls are exercised against their stub services beside their clients
//! (`blob.stub.test.rs`, `r2.stub.test.rs`).

use std::sync::Arc;

use super::*;
use crate::blob::{BlobClient, Credential};

fn runs() -> BlobClient {
    BlobClient::with_credential(
        "devstoreaccount1",
        "http://azurite:10000/devstoreaccount1",
        "runs",
        Credential::SharedKey(Arc::new(b"key".to_vec())),
        reqwest::Client::new(),
    )
}

#[test]
fn an_r2_config_opens_the_bucket() {
    let config = PublicStoreConfig::R2(R2Config {
        account_id: "acct".to_string(),
        bucket: "bucket".to_string(),
        access_key_id: "id".to_string(),
        secret_access_key: "secret".to_string(),
        endpoint: "https://acct.r2.cloudflarestorage.com".to_string(),
        region: "auto".to_string(),
    });
    assert_eq!(open(&config, &runs()).kind(), "r2");
}

#[test]
fn a_blob_config_opens_a_container_of_the_run_account() {
    let config = PublicStoreConfig::Blob {
        container: "public".to_string(),
    };
    assert_eq!(open(&config, &runs()).kind(), "blob");
}

#[tokio::test]
async fn the_memory_store_lists_a_prefix_with_sizes_in_name_order() {
    let store = MemoryPublicStore::new();
    store.insert("media/b", b"22", "text/plain");
    store.insert("media/a", b"1", "text/plain");
    store.insert("other/c", b"333", "text/plain");
    let listed = store.list("media/").await.unwrap();
    assert_eq!(
        listed,
        [
            PublicObject {
                key: "media/a".to_string(),
                bytes: 1,
                last_modified: Some(MEMORY_LAST_MODIFIED),
            },
            PublicObject {
                key: "media/b".to_string(),
                bytes: 2,
                last_modified: Some(MEMORY_LAST_MODIFIED),
            },
        ]
    );
    assert_eq!(store.list("").await.unwrap().len(), 3);
    assert!(store.list("none/").await.unwrap().is_empty());
    assert_eq!(
        store.list_keys("media/").await.unwrap(),
        ["media/a", "media/b"]
    );
}

#[tokio::test]
async fn the_memory_store_deletes_idempotently_and_records_each_delete() {
    let store = MemoryPublicStore::new();
    store.insert("a", b"1", "text/plain");
    store.insert("b", b"2", "text/plain");
    store.delete("a").await.unwrap();
    store.delete("a").await.unwrap();
    store.delete("never").await.unwrap();
    assert_eq!(store.keys(), ["b"]);
    assert_eq!(store.deletes(), ["a", "a", "never"]);
    assert!(store.writes().is_empty());
}
