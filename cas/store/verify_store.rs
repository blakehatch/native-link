// Copyright 2022 The Turbo Cache Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use ac_utils::get_and_decode_digest;
use async_trait::async_trait;
use proto::build::bazel::remote::execution::v2::Directory;
use sha2::{Digest, Sha256};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use common::{log, DigestInfo};
use error::{make_input_err, Code, Error, ResultExt};
use metrics_utils::{Collector, CollectorState, CounterWithTime, MetricsComponent, Registry};
use traits::{StoreTrait, UploadSizeInfo};

pub struct VerifyStore {
    inner_store: Arc<dyn StoreTrait>,
    existence_cache: Mutex<HashSet<DigestInfo>>,
    verify_size: bool,
    verify_hash: bool,

    // Metrics.
    size_verification_failures: CounterWithTime,
    hash_verification_failures: CounterWithTime,
}

impl VerifyStore {
    pub fn new(config: &config::stores::VerifyStore, inner_store: Arc<dyn StoreTrait>) -> Self {
        VerifyStore {
            inner_store,
            existence_cache: Mutex::new(HashSet::new()),
            verify_size: config.verify_size,
            verify_hash: config.verify_hash,
            size_verification_failures: CounterWithTime::default(),
            hash_verification_failures: CounterWithTime::default(),
        }
    }

    pub async fn record_existence_directory_walk(
        self: Arc<Self>,
        digest: DigestInfo,
    ) -> Box<dyn Future<Output = Result<(), Error>>> {
        Box::new(async move {
            // Fetch the Directory object from the CAS using get_and_decode_digest
            let directory = get_and_decode_digest::<Directory>(self.pin_inner(), &digest).await?;

            // Iterate over the files and directories in this Directory
            for file in &directory.files {
                // For each file, check its existence in the CAS
                let file_digest = DigestInfo::try_new(
                    &file.digest.as_ref().map_or("", |d| &d.hash),
                    file.digest.as_ref().map_or(0, |d| d.size_bytes),
                )?;
                let cache_lock = self.existence_cache.lock();
                match cache_lock {
                    Ok(mut cache) => {
                        if cache.contains(&file_digest) {
                            continue;
                        }
                        let exists = self
                            .pin_inner()
                            .has(file_digest)
                            .await
                            .err_tip(|| "Failed to check file existence in CAS")?;
                        if let Some(_) = exists {
                            cache.insert(file_digest);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to acquire lock on existence_cache: {}", e);
                    }
                }
            }

            for child_directory in &directory.directories {
                // For each child directory, recursively walk it
                let child_digest = child_directory.digest.as_ref().map_or_else(
                    || Err(Error::new(Code::Internal, "Digest is None".to_string())),
                    |digest| DigestInfo::try_new(&digest.hash.clone(), digest.size_bytes as usize).map_err(Error::from),
                )?;

                let _child_result = self.clone().record_existence_directory_walk(child_digest).await;
            }

            Ok(())
        })
    }

    pub async fn has_in_cache(&self, digest: &DigestInfo) -> bool {
        match self.existence_cache.lock() {
            Ok(cache) => cache.contains(digest),
            Err(_) => false,
        }
    }

    fn pin_inner(&self) -> Pin<&dyn StoreTrait> {
        Pin::new(self.inner_store.as_ref())
    }

    async fn inner_check_update(
        &self,
        mut tx: DropCloserWriteHalf,
        mut rx: DropCloserReadHalf,
        size_info: UploadSizeInfo,
        mut maybe_hasher: Option<([u8; 32], Sha256)>,
    ) -> Result<(), Error> {
        let mut sum_size: u64 = 0;
        loop {
            let chunk = rx
                .recv()
                .await
                .err_tip(|| "Failed to reach chunk in check_update in verify store")?;
            sum_size += chunk.len() as u64;

            if chunk.is_empty() {
                // Is EOF.
                if let UploadSizeInfo::ExactSize(expected_size) = size_info {
                    if sum_size != expected_size as u64 {
                        self.size_verification_failures.inc();
                        return Err(make_input_err!(
                            "Expected size {} but got size {} on insert",
                            expected_size,
                            sum_size
                        ));
                    }
                }
                if let Some((original_hash, hasher)) = maybe_hasher {
                    let hash_result: [u8; 32] = hasher.finalize().into();
                    if original_hash != hash_result {
                        self.hash_verification_failures.inc();
                        return Err(make_input_err!(
                            "Hashes do not match, got: {} but digest hash was {}",
                            hex::encode(original_hash),
                            hex::encode(hash_result),
                        ));
                    }
                }
                tx.send_eof().await.err_tip(|| "In verify_store::check_update")?;
                break;
            }

            // This will allows us to hash while sending data to another thread.
            let write_future = tx.send(chunk.clone());

            if let Some((_, hasher)) = maybe_hasher.as_mut() {
                hasher.update(chunk.as_ref());
            }

            write_future
                .await
                .err_tip(|| "Failed to write chunk to inner store in verify store")?;
        }
        Ok(())
    }
}

#[async_trait]
impl StoreTrait for VerifyStore {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[DigestInfo],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        for (i, digest) in digests.iter().enumerate() {
            let digest = digest.clone();
            if self.existence_cache.lock().unwrap().contains(&digest) {
                results[i] = Some(1);
            } else {
                let result = self
                    .pin_inner()
                    .has_with_results(&[digest], &mut results[i..i + 1])
                    .await;
                self.existence_cache.lock().unwrap().insert(digest);
                result?
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let digest_size =
            usize::try_from(digest.size_bytes).err_tip(|| "Digest size_bytes was not convertible to usize")?;
        if let UploadSizeInfo::ExactSize(expected_size) = size_info {
            if self.verify_size && expected_size != digest_size {
                self.size_verification_failures.inc();
                return Err(make_input_err!(
                    "Expected size to match. Got {} but digest says {} on update",
                    expected_size,
                    digest.size_bytes
                ));
            }
        }

        let mut hasher = None;
        if self.verify_hash {
            hasher = Some((digest.packed_hash, Sha256::new()));
        }

        let (tx, rx) = make_buf_channel_pair();

        let update_fut = self.pin_inner().update(digest, rx, size_info);
        let check_fut = self.inner_check_update(tx, reader, size_info, hasher);

        let (update_res, check_res) = tokio::join!(update_fut, check_fut);

        update_res.merge(check_res)
    }

    async fn get_part_ref(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        self.pin_inner().get_part_ref(digest, writer, offset, length).await
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(self)
    }

    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self)));
    }
}

impl MetricsComponent for VerifyStore {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "verify_size_enabled",
            &self.verify_size,
            "If the verification store is verifying the size of the data",
        );
        c.publish(
            "verify_hash_enabled",
            &self.verify_hash,
            "If the verification store is verifying the hash of the data",
        );
        c.publish(
            "size_verification_failures_total",
            &self.size_verification_failures,
            "Number of failures the verification store had due to size mismatches",
        );
        c.publish(
            "hash_verification_failures_total",
            &self.hash_verification_failures,
            "Number of failures the verification store had due to hash mismatches",
        );
    }
}
