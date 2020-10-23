import * as fs from 'fs';
import * as assert from 'assert';
import { Client } from 'pg';
import * as c32check from 'c32check';

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

const GET_PLACEHOLDER_ACCOUNTS_QUERY = `
  SELECT address, accts.amount, accts.block_height
  FROM (
    SELECT DISTINCT ON (address)
      address, lock_transfer_block_id as block_height, (credit_value::numeric - debit_value::numeric) amount
    FROM accounts 
    WHERE type = 'STACKS' AND NOT address !~ '(-|_)'
    AND credit_value::numeric - debit_value::numeric > 0
    ORDER BY address, block_id DESC, vtxindex DESC 
  ) accts
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

const GET_PLACEHOLDER_VESTING_ADDRESSES_QUERY = `
  SELECT address, vesting_value as amount, block_id as block_height
  FROM account_vesting
  WHERE type = 'STACKS' AND NOT address !~ '(-|_)'
`;

const LOCK_TRANSFER_BLOCK_IDS_QUERY = `
  SELECT distinct(lock_transfer_block_id) as block_id FROM ACCOUNTS
  ORDER BY lock_transfer_block_id ASC
`;

function isValidBtcAddress(address: string): boolean {
  if (address.length < 26 || address.length > 35) {
    return false;
  }
  try {
    const addr = c32check.b58ToC32(address)
    return !!addr;
  } catch (error) {
    return false;
  }
}

function microStxToStx(microStx: string): string {
  const padded = microStx.padStart(7, '0');
  const stxInt = padded.slice(0, -6);
  const stxFrac = padded.slice(-6);
  return `${stxInt}.${stxFrac}`;
}

function microStxToReadable(microStx: string): string {
  return microStx.padStart(7, '0').slice(0, -6).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

async function run() {
  const client = new Client();
  await client.connect()

  const currentBlockHeight = (await client.query<{block_id: number}>(STX_LATEST_BLOCK_HEIGHT_QUERY)).rows[0].block_id;
  const currentDate = Math.round(Date.now() / 1000)

  // get all account balances with placeholder addresses -- query ensures they are already unique
  const placeholderAccounts = (await client.query<{address: string; amount: string; block_height: string}>(GET_PLACEHOLDER_ACCOUNTS_QUERY))
    .rows
    .filter(r => !isValidBtcAddress(r.address));

  // get all vesting accounts with placeholder addresses -- these must be aggregated for total balances
  const placeholderVesting = (await client.query<{address: string; amount: string; block_height: string}>(GET_PLACEHOLDER_VESTING_ADDRESSES_QUERY))
    .rows
    .filter(r => !isValidBtcAddress(r.address));

  const totalPlaceholderBalance = [
    ...placeholderAccounts,
    ...placeholderVesting
  ].map(r => BigInt(r.amount)).reduce((a, b) => a + b);

  const placeholderMap = new Map<string, bigint>();
  [...placeholderAccounts, ...placeholderVesting].forEach(r => {
    const amount = (placeholderMap.get(r.address) ?? 0n) + BigInt(r.amount);
    placeholderMap.set(r.address, amount);
  });

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

  const placeholderBalance = microStxToReadable(totalPlaceholderBalance.toString());
  console.log(`Amount under placeholder accounts:\n${placeholderBalance}`);
  assert.strictEqual(placeholderBalance, '4,392,964', 'incorrect placeholder balance');

  const finalSupply = microStxToReadable(totals[totals.length - 1].total_calculated.toString());
  console.log(`Final supply:\n${finalSupply}`);
  assert.strictEqual(finalSupply, '1,352,464,598', 'incorrect final unlocked balance');

  // output to csv
  const fd = fs.openSync('supply.csv', 'w');
  fs.writeSync(fd, 'block_height,unlocked_micro_stx,unlocked_stx,estimated_time\r\n');
  for (const entry of totals) {
    const stxStr = microStxToStx(entry.total_calculated.toString());
    fs.writeSync(fd, `${entry.block_height},${entry.total_calculated},${stxStr},${entry.date_time}\r\n`);
  }
  fs.closeSync(fd);

  // output all entries for placeholder accounts and balances to CSV
  const placeholderEntriesFd = fs.openSync('placeholder-entries.csv', 'w');
  fs.writeSync(placeholderEntriesFd, 'type,unlock_or_vest_block,address,stx_amount\r\n');
  for (const entry of placeholderAccounts) {
    fs.writeSync(fd, `locked_or_liquid,${entry.block_height},${entry.address},${microStxToStx(entry.amount)}\r\n`);
  }
  for (const entry of placeholderVesting) {
    fs.writeSync(fd, `vesting,${entry.block_height},${entry.address},${microStxToStx(entry.amount)}\r\n`);
  }
  fs.closeSync(placeholderEntriesFd);

  // output aggregated (distinct) placeholder addresses and balance to CSV
  const placeholderUniqueFd = fs.openSync('placeholder-unique.csv', 'w');
  fs.writeSync(placeholderUniqueFd, 'address,stx_amount\r\n');
  for (const [address, stx] of placeholderMap) {
    fs.writeSync(fd, `${address},${microStxToStx(stx.toString())}\r\n`);
  }
  fs.closeSync(placeholderUniqueFd);

  await client.end()
}


run().catch(error => {
  console.error(error);
  throw error;
  process.exit(1);
});
