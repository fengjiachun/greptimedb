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

#![feature(iter_partition_in_place)]
#![feature(assert_matches)]

pub mod bitmap;
pub mod bloom_filter;
pub mod error;
pub mod external_provider;
pub mod fulltext_index;
pub mod inverted_index;

pub type Bytes = Vec<u8>;
pub type BytesRef<'a> = &'a [u8];
