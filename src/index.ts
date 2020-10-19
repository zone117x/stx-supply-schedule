import * as fs from 'fs';
import * as assert from 'assert';
import { Client } from 'pg';

const STX_TOTAL_TXS_AT_BLOCK_QUERY = `
  WITH totals AS (
    SELECT DISTINCT ON (address) credit_value, debit_value 
      FROM accounts 
      WHERE type = 'STACKS' 
      AND address !~ '(-|_)' 
      AND length(address) BETWEEN 33 AND 34 
      AND lock_transfer_block_id <= $1
      ORDER BY address, block_id DESC, vtxindex DESC 
  )
  SELECT SUM(
    CAST(totals.credit_value AS bigint) - CAST(totals.debit_value AS bigint)
  ) AS val FROM totals
`;


const STX_LATEST_TOTAL_TXS_QUERY = `
  WITH 
    block_height AS (SELECT MAX(block_id) from accounts),
    totals AS (
      SELECT DISTINCT ON (address) credit_value, debit_value 
        FROM accounts 
        WHERE type = 'STACKS' 
        AND address !~ '(-|_)' 
        AND length(address) BETWEEN 33 AND 34 
        AND lock_transfer_block_id <= (SELECT * from block_height) 
        ORDER BY address, block_id DESC, vtxindex DESC 
    )
  SELECT (SELECT * from block_height) AS val
  UNION ALL
  SELECT SUM(
    CAST(totals.credit_value AS bigint) - CAST(totals.debit_value AS bigint)
  ) AS val FROM totals
`;

const STX_VESTED_BY_BLOCK_QUERY = `
  SELECT SUM(CAST(vesting_value as bigint)) as micro_stx, block_id
  FROM account_vesting
  WHERE type = 'STACKS'
  GROUP BY block_id
  ORDER BY block_id ASC
`;

const LOCK_TRANSFER_BLOCK_IDS_QUERY = `
  SELECT distinct(lock_transfer_block_id) as block_id FROM ACCOUNTS
  ORDER BY lock_transfer_block_id ASC
`;

async function run() {
  const client = new Client();
  await client.connect()

  const initSupplyRes = await client.query<{val: string}>(STX_LATEST_TOTAL_TXS_QUERY);
  const initBlockHeight = parseInt(initSupplyRes.rows[0].val);
  const initStxSupply = BigInt(initSupplyRes.rows[1].val);

  // block heights where vesting stx unlock (and become liquid)
  const vestedByBlockRes = await client.query<{block_id: string, micro_stx: string;}>(STX_VESTED_BY_BLOCK_QUERY);
  const vestingResults = vestedByBlockRes.rows.map(r => ({
    block_height: parseInt(r.block_id),
    micro_stx: BigInt(r.micro_stx)
  }));
  const vestingBlockHeights = vestingResults.map(r => r.block_height);

  // block heights where stx transfer txs unlock (and become liquid)
  const lockTransferBlockHeightsRes = await client.query<{block_id: string}>(LOCK_TRANSFER_BLOCK_IDS_QUERY);
  const lockTransferBlockHeights = lockTransferBlockHeightsRes.rows.map(r => parseInt(r.block_id));

  // the last block height where stx liquid supply changes
  const lastBlockHeight = Math.max(lockTransferBlockHeights.slice(-1)[0], vestingBlockHeights.slice(-1)[0]) + 5;

  // final unlocked supplies by block height
  const totals: { block_height: number; queried_micro_stx: bigint; vested_micro_stx: bigint; total_calculated: bigint }[] = [];
  for (let blockHeight = initBlockHeight; blockHeight < lastBlockHeight; blockHeight++) {
    const total = { block_height: blockHeight, queried_micro_stx: 0n, vested_micro_stx: 0n, total_calculated: 0n };
    total.vested_micro_stx = totals[totals.length - 1]?.vested_micro_stx ?? 0n;
    const vestingBlock = vestingResults.find(r => r.block_height === blockHeight);
    if (vestingBlock) {
      total.vested_micro_stx += vestingBlock.micro_stx;
    }
    if (lockTransferBlockHeights.includes(blockHeight)) {
      const res = await client.query<{val: string}>(STX_TOTAL_TXS_AT_BLOCK_QUERY, [blockHeight]);
      total.queried_micro_stx = BigInt(res.rows[0].val);
    } if (total.block_height === initBlockHeight) {
      total.queried_micro_stx = initStxSupply;
    } else {
      // no change in unlocked balances so use previous block's
      total.queried_micro_stx = totals[totals.length - 1].queried_micro_stx;
    }
    total.total_calculated = total.queried_micro_stx + total.vested_micro_stx;
    totals.push(total);
  }

  // sanity checks
  assert(totals[0].block_height === initBlockHeight, 'unexpected initial block height');
  for (let i = 1; i < totals.length; i++) {
    try {
      assert(totals[i].block_height === totals[i - 1].block_height + 1, 'unordered blocks!');
      assert(totals[i].total_calculated >= totals[i - 1].total_calculated, 'total balance should always increase!');
    } catch (error) {
      console.error(error);
      throw error;
    }
  }

  // output to csv
  const fd = fs.openSync('supply.csv', 'w');
  fs.writeSync(fd, 'block_height,unlocked_micro_stx,unlocked_stx,vested\r\n');
  for (const entry of totals) {
    const stxInt = entry.total_calculated.toString().slice(0, -6);
    const stxFrac = entry.total_calculated.toString().slice(-6);
    const stxStr = `${stxInt}.${stxFrac}`;
    fs.writeSync(fd, `${entry.block_height},${entry.total_calculated},${stxStr},${entry.vested_micro_stx}\r\n`);
  }
  fs.closeSync(fd);

  await client.end()
}


run().catch(error => {
  console.error(error);
  throw error;
});
