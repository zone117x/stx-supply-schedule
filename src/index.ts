import * as fs from 'fs';
import * as assert from 'assert';
import { Client } from 'pg';

const STX_TOTAL_AT_BLOCK_QUERY = `
  SELECT SUM(balances.credit_value::numeric - balances.debit_value::numeric) balance
  FROM (
    SELECT DISTINCT ON (address) address, credit_value, debit_value 
    FROM accounts 
    WHERE type = 'STACKS' 
    AND lock_transfer_block_id <= $1
    AND block_id <= $1
    ORDER BY address, block_id DESC, vtxindex DESC 
  ) balances
`;

const STX_LATEST_BLOCK_HEIGHT_QUERY = `
  SELECT MAX(block_id) as block_id FROM accounts
`;

const STX_VESTED_BY_BLOCK_QUERY = `
  SELECT SUM(vesting_value::numeric) as micro_stx, block_id
  FROM account_vesting
  WHERE type = 'STACKS'
  GROUP BY block_id
  ORDER BY block_id ASC
`;

const STX_TOTAL_VESTED_BY_BLOCK_QUERY = `
  SELECT SUM(vesting_value::numeric) as micro_stx
  FROM account_vesting
  WHERE type = 'STACKS' AND block_id <= $1 AND block_id > $2
`;

const LOCK_TRANSFER_BLOCK_IDS_QUERY = `
  SELECT distinct(lock_transfer_block_id) as block_id FROM ACCOUNTS
  ORDER BY lock_transfer_block_id ASC
`;

async function run() {
  const client = new Client();
  await client.connect()

  const currentBlockHeight = (await client.query<{block_id: number}>(STX_LATEST_BLOCK_HEIGHT_QUERY)).rows[0].block_id;
  const currentDate = Math.round(Date.now() / 1000)

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
  const totals: { block_height: number; queried_micro_stx: bigint; vested_micro_stx: bigint; total_calculated: bigint; date_time: string }[] = [];
  for (let blockHeight = currentBlockHeight; blockHeight < lastBlockHeight; blockHeight++) {
    const blockTimePassed = (blockHeight - currentBlockHeight) * 10 * 60;
    const blockTimestamp = new Date((currentDate + blockTimePassed) * 1000).toISOString();
    const total = { 
      block_height: blockHeight, 
      queried_micro_stx: 0n, 
      vested_micro_stx: 0n, 
      total_calculated: 0n, 
      date_time: blockTimestamp
    };

    if (vestingBlockHeights.includes(blockHeight)) {
      // stx vesting at this block, query for total vested amount up until this block
      const vestingRes = await client.query<{micro_stx: string}>(STX_TOTAL_VESTED_BY_BLOCK_QUERY, [blockHeight, currentBlockHeight]);
      total.vested_micro_stx = BigInt(vestingRes.rows[0].micro_stx);
    } else {
      // no stx vesting at this block, so reuse last known total vested amount
      total.vested_micro_stx = totals[totals.length - 1]?.vested_micro_stx ?? 0n;
    }

    if (lockTransferBlockHeights.includes(blockHeight) || total.block_height === currentBlockHeight) {
      const res = await client.query<{balance: string}>(STX_TOTAL_AT_BLOCK_QUERY, [blockHeight]);
      total.queried_micro_stx = BigInt(res.rows[0].balance);
    } else {
      // no change in unlocked balances so use previous block's
      total.queried_micro_stx = totals[totals.length - 1].queried_micro_stx;
    }
    total.total_calculated = total.queried_micro_stx + total.vested_micro_stx;
    totals.push(total);
  }

  // sanity checks
  assert(totals[0].block_height === currentBlockHeight, 'unexpected initial block height');
  for (let i = 1; i < totals.length; i++) {
    try {
      assert(totals[i].block_height === totals[i - 1].block_height + 1, 'unordered blocks!');
      assert(totals[i].total_calculated >= totals[i - 1].total_calculated, 'total balance should always increase!');
    } catch (error) {
      console.error(error);
      throw error;
    }
  }

  // prune blocks where supply didn't change
  for (let i = totals.length - 1; i > 0; i--) {
    if (totals[i].total_calculated === totals[i - 1].total_calculated) {
      totals.splice(i, 1);
    }
  }

  const finalSupply = totals[totals.length - 1].total_calculated.toString()
    .slice(0, -6)
    .replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  console.log(`Final supply:\n${finalSupply}`);
  assert.strictEqual(finalSupply, '1,352,464,598', 'incorrect final unlocked balance');

  // output to csv
  const fd = fs.openSync('supply.csv', 'w');
  fs.writeSync(fd, 'block_height,unlocked_micro_stx,unlocked_stx,estimated_time\r\n');
  for (const entry of totals) {
    const stxInt = entry.total_calculated.toString().slice(0, -6);
    const stxFrac = entry.total_calculated.toString().slice(-6);
    const stxStr = `${stxInt}.${stxFrac}`;
    fs.writeSync(fd, `${entry.block_height},${entry.total_calculated},${stxStr},${entry.date_time}\r\n`);
  }
  fs.closeSync(fd);

  await client.end()
}


run().catch(error => {
  console.error(error);
  throw error;
  process.exit(1);
});
