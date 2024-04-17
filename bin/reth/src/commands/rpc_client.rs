use alloy_rlp::Decodable;
use ethers_providers::{Http, Middleware, Provider};
use itertools::Either;
use reth_interfaces::p2p::{
    bodies::client::{BodiesClient, BodiesFut},
    download::DownloadClient,
    error::RequestError,
    headers::client::{HeadersClient, HeadersFut, HeadersRequest},
    priority::Priority,
};


use ethers_core::types::{Block as EthersBlock, Transaction as EthersTransaction};
use reth_primitives::{
    U256, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber, Header, HeadersDirection, PeerId, TransactionSigned, B256
};
use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};
use tokio::sync::Semaphore;
use thiserror::Error;

use futures::future;

use tracing::{trace, warn};

const CHUNK_SIZE: usize = 1000;
const CONCURRENCY: usize = 100;

/// Front-end API for fetching chain data from a file.
///
/// Blocks are assumed to be written one after another in a file, as rlp bytes.
///
/// For example, if the file contains 3 blocks, the file is assumed to be encoded as follows:
/// rlp(block1) || rlp(block2) || rlp(block3)
///
/// Blocks are assumed to have populated transactions, so reading headers will also buffer
/// transactions in memory for use in the bodies stage.
///
/// This reads the entire file into memory, so it is not suitable for large files.
#[derive(Debug)]
pub struct RPCClient {
    provider: Arc<Provider<Http>>,

    start: u64,
    end: u64,

    /// The buffered headers retrieved when fetching new bodies.
    headers: HashMap<BlockNumber, Header>,

    /// A mapping between block hash and number.
    hash_to_number: HashMap<BlockHash, BlockNumber>,

    /// The buffered bodies retrieved when fetching new headers.
    bodies: HashMap<BlockHash, BlockBody>,

    tip: Option<B256>,

}

#[derive(Debug, Error)]
pub enum FileClientError {
    /// An error occurred when opening or reading the file.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// An error occurred when decoding blocks, headers, or rlp headers from the file.
    #[error(transparent)]
    Rlp(#[from] alloy_rlp::Error),

}

impl RPCClient {
    /// Create a new RPC client from an RPC url
    pub async fn create(rpc_url: &String, start: u64, end: u64) -> eyre::Result<Self> {
        let provider = Arc::new(Provider::<Http>::try_from(rpc_url)?);
        let mut client = RPCClient { tip: None, provider, start, end, headers: HashMap::new(), hash_to_number: HashMap::new(), bodies: HashMap::new() };
        client.load().await?;
        Ok(client)
    }

    /// Returns the highest block number of this client has or `None` if empty
    pub fn max_block(&self) -> Option<u64> {
        return Some(self.end)
    }

    /// Returns true if all blocks are canonical (no gaps)
    pub fn has_canonical_blocks(&self) -> bool {
        if self.headers.is_empty() {
            return true
        }
        let mut nums = self.headers.keys().copied().collect::<Vec<_>>();
        nums.sort_unstable();
        let mut iter = nums.into_iter();
        let mut lowest = iter.next().expect("not empty");
        for next in iter {
            if next != lowest + 1 {
                return false
            }
            lowest = next;
        }
        true
    }

    pub fn tip(&self) -> Option<B256> {
        self.tip
    }

    /// Use the provided bodies as the file client's block body buffer.
    pub fn with_bodies(mut self, bodies: HashMap<BlockHash, BlockBody>) -> Self {
        self.bodies = bodies;
        self
    }

    /// Use the provided headers as the file client's block body buffer.
    pub fn with_headers(mut self, headers: HashMap<BlockNumber, Header>) -> Self {
        self.headers = headers;
        for (number, header) in &self.headers {
            self.hash_to_number.insert(header.hash_slow(), *number);
        }
        self
    }

    /// Load the headers and bodies for the file client.
    pub async fn load(&mut self) -> eyre::Result<()> {
        let blocks = RangeInclusive::new(self.start, self.end).collect::<Vec<_>>();
        for chunk in blocks.chunks(CHUNK_SIZE) {

            let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
            let mut futures = Vec::new();

            for &bn in chunk {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("Failed to acquire semaphore");
                let provider = self.provider.clone();
                futures.push(tokio::spawn(async move {
                    let block = provider
                        .get_block_with_txs(bn)
                        .await?
                        .ok_or(eyre::eyre!("not found: {}", bn))?;
                    drop(permit);
                    Result::<_, eyre::Error>::Ok(ethers_block_to_block(block)?)
                }));
            }

            let results = future::join_all(futures).await;
            for result in results {
                let (header, body) = result??;

                

                self.hash_to_number.insert(header.hash_slow(), header.number);
                self.bodies.insert(header.hash_slow(), body);
                self.headers.insert(header.number, header);
            }
        }

        self.tip = self.headers.get(&self.end).map(|h| h.hash_slow());



        Ok(())

    }
}


impl HeadersClient for RPCClient {
    type Output = HeadersFut;

    fn get_headers_with_priority(
        &self,
        request: HeadersRequest,
        _priority: Priority,
    ) -> Self::Output {
        // this just searches the buffer, and fails if it can't find the header
        let mut headers = Vec::new();
        trace!(target: "downloaders::file", request=?request, "Getting headers");

        let start_num = match request.start {
            BlockHashOrNumber::Hash(hash) => match self.hash_to_number.get(&hash) {
                Some(num) => *num,
                None => {
                    warn!(%hash, "Could not find starting block number for requested header hash");
                    return Box::pin(async move { Err(RequestError::BadResponse) })
                }
            },
            BlockHashOrNumber::Number(num) => num,
        };

        let range = if request.limit == 1 {
            Either::Left(start_num..start_num + 1)
        } else {
            match request.direction {
                HeadersDirection::Rising => Either::Left(start_num..start_num + request.limit),
                HeadersDirection::Falling => {
                    Either::Right((start_num - request.limit + 1..=start_num).rev())
                }
            }
        };

        trace!(target: "downloaders::file", range=?range, "Getting headers with range");

        for block_number in range {
            match self.headers.get(&block_number).cloned() {
                Some(header) => headers.push(header),
                None => {
                    warn!(number=%block_number, "Could not find header");
                    return Box::pin(async move { Err(RequestError::BadResponse) })
                }
            }
        }

        Box::pin(async move { Ok((PeerId::default(), headers).into()) })
    }
}

impl BodiesClient for RPCClient {
    type Output = BodiesFut;

    fn get_block_bodies_with_priority(
        &self,
        hashes: Vec<B256>,
        _priority: Priority,
    ) -> Self::Output {
        // this just searches the buffer, and fails if it can't find the block
        let mut bodies = Vec::new();

        // check if any are an error
        // could unwrap here
        for hash in hashes {
            match self.bodies.get(&hash).cloned() {
                Some(body) => bodies.push(body),
                None => {
                    warn!(hash=%hash, "Could not find header");
                    return return Box::pin(async move { Err(RequestError::BadResponse) })
                },
            }
        }

        Box::pin(async move { Ok((PeerId::default(), bodies).into()) })
    }
}

impl DownloadClient for RPCClient {
    fn report_bad_message(&self, _peer_id: PeerId) {
        warn!("Reported a bad message on a file client, the file may be corrupted or invalid");
        // noop
    }

    fn num_connected_peers(&self) -> usize {
        // no such thing as connected peers when we are just using a file
        1
    }
}


fn ethers_block_to_block(block: EthersBlock<EthersTransaction>) -> eyre::Result<(Header, BlockBody)> {
    let header = Header {
        parent_hash: block.parent_hash.0.into(),
        number: block.number.unwrap().as_u64(),
        gas_limit: block.gas_limit.as_u64(),
        difficulty: U256::from_limbs(block.difficulty.0),
        nonce: block.nonce.unwrap().to_low_u64_be(),
        extra_data: block.extra_data.0.clone().into(),
        state_root: block.state_root.0.into(),
        transactions_root: block.transactions_root.0.into(),
        receipts_root: block.receipts_root.0.into(),
        timestamp: block.timestamp.as_u64(),
        mix_hash: block.mix_hash.unwrap().0.into(),
        beneficiary: block.author.unwrap().0.into(),
        base_fee_per_gas: block.base_fee_per_gas.map(|fee| fee.as_u64()),
        ommers_hash: block.uncles_hash.0.into(),
        gas_used: block.gas_used.as_u64(),
        withdrawals_root: block.withdrawals_root.map(|b| b.0.into()),
        logs_bloom: block.logs_bloom.unwrap_or_default().0.into(),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None
    };
    let mut body: Vec<TransactionSigned> = vec![];
    for tx in block.transactions {
        let rlp = tx.rlp();
        let mut bytes: &[u8] = rlp.0.as_ref();
        let tx2 = TransactionSigned::decode(&mut bytes)
            .map_err(|e| eyre::eyre!("could not decode: {}", e))?;
        body.push(tx2);
    }
    Ok((
        header,
        BlockBody {
            transactions: body,
            ommers: vec![],
            withdrawals: Some(Withdrawals::new(vec![])),
        }
    ))
}
