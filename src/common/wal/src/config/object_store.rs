// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};

/// Object store wal configurations for datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ObjectStoreWalConfig {
    /// Name of the storage provider that holds the WAL objects.
    /// An empty name selects the default object store.
    pub storage_provider: String,
    /// Path prefix of the WAL objects inside the storage provider.
    pub prefix: String,
    /// Interval of flushing buffered entries to the object store.
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// The max size of a single batch object.
    pub max_batch_bytes: ReadableSize,
}

impl Default for ObjectStoreWalConfig {
    fn default() -> Self {
        Self {
            storage_provider: String::new(),
            prefix: "wal".to_string(),
            flush_interval: Duration::from_secs(1),
            max_batch_bytes: ReadableSize::mb(1),
        }
    }
}
