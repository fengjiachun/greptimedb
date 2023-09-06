// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::meta::MailboxMessage;
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::{CacheInvalidator, Context};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::ident::TableIdent;
use common_meta::instruction::Instruction;
use snafu::ResultExt;

use crate::metasrv::MetasrvInfo;
use crate::service::mailbox::{BroadcastChannel, MailboxRef};

const DEFAULT_SUBJECT: &str = "Invalidate table";

pub struct MetasrvCacheInvalidator {
    mailbox: MailboxRef,
    // Metasrv infos
    info: MetasrvInfo,
}

impl MetasrvCacheInvalidator {
    pub fn new(mailbox: MailboxRef, info: MetasrvInfo) -> Self {
        Self { mailbox, info }
    }
}

#[async_trait::async_trait]
impl CacheInvalidator for MetasrvCacheInvalidator {
    async fn invalidate_table(&self, ctx: &Context, table_ident: TableIdent) -> MetaResult<()> {
        let instruction = Instruction::InvalidateTableCache(table_ident);
        let subject = &ctx
            .subject
            .clone()
            .unwrap_or_else(|| DEFAULT_SUBJECT.to_string());

        let msg = &MailboxMessage::json_message(
            subject,
            &format!("Metasrv@{}", self.info.server_addr),
            "Frontend broadcast",
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| meta_error::SerdeJsonSnafu)?;

        self.mailbox
            .broadcast(&BroadcastChannel::Frontend, msg)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}
