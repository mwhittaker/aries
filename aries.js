// ARIES: Algorithm for Recovery and Isolation Exploiting Semantics [1, 2].
//
// Relational databases support transactions with guarantees of atomicity,
// consistency, isolation, and durability. Consistency is provided (partially)
// by keys, constraints, and triggers. Isolation is provided by some form
// concurrency control like two-phase locking. Atomicity and durability are the
// responsibility of a database' recovery procedure. The recovery procedure is
// responsible for ensuring that committed data is persisted and dually for
// ensuring that non-committed data is not persisted. It must provide these
// guarantees even in the event of repeated failures.
//
// ARIES is an industry strength recovery procedure developed at IBM. It
// employs write-ahead logging, a bunch of data structures, and a three phase
// algorithm. At any given point of execution, the state of ARIES may look
// something like this:
//
// Dirty Page Table                Log
// +--------+--------+             +-----+---------+--------+--------+---------+
// | pageID | recLSN |             | LSN | transID | type   | pageID | prevLSN |
// +--------+--------+             +-----+---------+--------+--------+---------+
// | A      | 0      |             | 0   | T1      | update | A      | null    |
// | B      | 9      |             | 1   | T2      | update | B      | null    |
// | C      | 7      |             | 2   | T3      | update | C      | null    |
// +--------+--------+             | 3   | T2      | update | D      | 1       |
//                                 | 4   | T1      | update | A      | 0       |
// Transaction Table               | 5   | T1      | commit |        | 4       |
// +---------+--------+---------+  | 6   | T1      | end    |        | 5       |
// | transID | status | lastLSN |  | 7   | T3      | update | C      | 2       |
// +---------+--------+---------+  | 8   | T2      | update | D      | 3       |
// | T2      | run    | 9       |  | 9   | T2      | update | B      | 8       |
// | T3      | run    | 10      |  | 10  | T3      | update | A      | 7       |
// +---------+--------+---------+  +-----+---------+--------+--------+---------+
//
// Buffer Pool                     Disk
// +--------+---------+-------+    +--------+---------+-------+
// | pageID | pageLSN | value |    | pageID | pageLSN | value |
// +--------+---------+-------+    +--------+---------+-------+
// | A      | 10      | "foo" |    | A      |         | ""    |
// | B      | 9       | "bar" |    | B      | 1       | "jar" |
// | C      | 7       | "baz" |    | C      | 2       | "yaz" |
// +--------+---------+-------+    | D      | 8       | "moo" |
//                                 +--------+---------+-------+
//
// This file implements a simplified version of the ARIES recovery procedure
// and is meant to help others learn the basics of how ARIES works.
//
// [1]: https://goo.gl/WsauYU
// [2]: http://pages.cs.wisc.edu/~dbbook/
//
// TODO(mwhittaker): Make functions check that none of their inputs are
// undefined.
// TODO(larry): Add visualization.

// The aries global namespace.
var aries = {};

// Types ///////////////////////////////////////////////////////////////////////
// Operations.
//
// ARIES processes a sequence of writes, commits, checkpoints, and page flushes
// from a number of transactions. For example, ARIES may process the following
// sequence of operations:
//
//   - W_1(A, "foo") // Transaction 1 writes value "foo" to page A.
//   - W_2(B, "bar") // Transaction 2 writes value "bar" to page B.
//   - Commit_1()    // Transaction 1 commits.
//   - Flush(B)      // Page B is flushed to disk.
//
// Note that there is an operation for writes but not for reads. Since reads do
// not mutate the state of the database, they can be ignored by ARIES.
// Operations are represented by the following types:
//
//   type Op.Type =
//     | WRITE
//     | COMMIT
//     | FLUSH
//     | CHECKPOINT
//
//   type Op.Operation = {
//     type:    aries.Op.Type,
//     txn_id:  string,
//     page_id: string,
//     value:   string
//   }
//
//   | type       | txn_id | page_id | value |
//   | ---------- | ------ | ------- | ----- |
//   | write      | y      | y       | y     |
//   | commit     | y      | n       | n     |
//   | flush      | n      | y       | n     |
//   | checkpoint | n      | n       | n     |
//
// For example, the sequence of operations above would be represented as:
//
//   - {type: WRITE,  txn_id:"1", page_id:"A", value:"foo"}
//   - {type: WRITE,  txn_id:"2", page_id:"B", value:"bar"}
//   - {type: COMMIT, txn_id:"1"                          }
//   - {type: FLUSH,              page_id:"B"             }
aries.Op = {};

aries.Op.Type = {
  WRITE:      "write",
  COMMIT:     "commit",
  FLUSH:      "flush",
  CHECKPOINT: "checkpoint",
};

aries.Op.Operation = function(type) {
  this.type = type;
}

aries.Op.Write = function(txn_id, page_id, value) {
  aries.Op.Operation.call(this, aries.Op.Type.WRITE);
  this.txn_id = txn_id;
  this.page_id = page_id;
  this.value = value;
}

aries.Op.Commit = function(txn_id) {
  aries.Op.Operation.call(this, aries.Op.Type.COMMIT);
  this.txn_id = txn_id;
}

aries.Op.Flush = function(page_id) {
  aries.Op.Operation.call(this, aries.Op.Type.FLUSH);
  this.page_id = page_id;
}

aries.Op.Checkpoint = function() {
  aries.Op.Operation.call(this, aries.Op.Type.CHECKPOINT);
}

// Log.
//
// The main ARIES data structure is a log. As ARIES runs, it appends log
// entries to the tail of the log which is kept in memory. Certain operations
// force certain prefixes of the log to be forced to disk. For example, when a
// transaction commits, the log is flushed to disk. A log entry is either an
// update, commit, end, CLR, or checkpoint. Every log entry has a log sequence
// number (LSN) and a type, but each log entry has a different set of data
// associated with it. The log is represented by the following types:
//
//   type Log.Entry = {
//     lsn:              number,
//     type:             aries.Log.Type,
//     txn_id:           string,
//     page_id:          string,
//     before:           string,
//     after:            string,
//     undo_next_lsn:    number,
//     dirty_page_table: DirtyPageTable,
//     txn_table:        TxnTable,
//     prev_lsn:         number, // or undefined
//   }
//
//   | command          | update | commit | end | clr | checkpoint |
//   | ---------------- | ------ | ------ | --- | --- | ---------- |
//   | lsn              | y      | y      | y   | y   | y          |
//   | type             | y      | y      | y   | y   | y          |
//   | txn_id           | y      | y      | y   | y   |            |
//   | page_id          | y      |        |     | y   |            |
//   | before           | y      |        |     |     |            |
//   | after            | y      |        |     | y   |            |
//   | undo_next_lsn    |        |        |     | y   |            |
//   | dirty_page_table |        |        |     |     | y          |
//   | txn_table        |        |        |     |     | y          |
//   | prev_lsn         | y      | y      | y   | y   |            |
//
// For convenience, a log entry's LSN is the same as its index in the log.
aries.Log = {};

aries.Log.Type = {
  UPDATE:     "update",
  COMMIT:     "commit",
  END:        "end",
  CLR:        "clr",
  CHECKPOINT: "checkpoint",
}

aries.Log.Entry = function(type, lsn) {
  this.type = type;
  this.lsn = lsn;
}

aries.Log.Update = function(lsn, txn_id, page_id, before, after, prev_lsn) {
  aries.Log.Entry.call(this, aries.Log.Type.UPDATE, lsn);
  this.txn_id = txn_id;
  this.page_id = page_id;
  this.before = before;
  this.after = after;
  this.prev_lsn = prev_lsn;
}

aries.Log.Commit = function(lsn, txn_id, prev_lsn) {
  aries.Log.Entry.call(this, aries.Log.Type.COMMIT, lsn);
  this.txn_id = txn_id;
  this.prev_lsn = prev_lsn;
}

aries.Log.End = function(lsn, txn_id, prev_lsn) {
  aries.Log.Entry.call(this, aries.Log.Type.END, lsn);
  this.txn_id = txn_id;
  this.prev_lsn = prev_lsn;
}

aries.Log.CLR = function(lsn, txn_id, page_id, after, undo_next_lsn, prev_lsn) {
  aries.Log.Entry.call(this, aries.Log.Type.CLR, lsn);
  this.txn_id = txn_id;
  this.page_id = page_id;
  this.after = after;
  this.undo_next_lsn = undo_next_lsn;
  this.prev_lsn = prev_lsn;
}

aries.Log.Checkpoint = function(lsn, dirty_page_table, txn_table) {
  aries.Log.Entry.call(this, aries.Log.Type.CHECKPOINT, lsn);
  this.dirty_page_table = dirty_page_table;
  this.txn_table = txn_table;
}

// Transaction Table.
//
// The transaction table records whether every transaction is in progress,
// committed, or aborted. It also records, for each transaction, the LSN of the
// most recent log entry that was performed by the transaction. This is dubbed
// the lastLSN of a transaction. The transaction table is represented by the
// following types:
//
//   type TxnTableEntry = {
//     txn_status: aries.TxnStatus,
//     last_lsn:   number,
//   }
//
//   type TxnTable = txn_id: string -> TxnTableEntry
//
// TxnTable is a map (i.e. Javascript object) mapping strings to TxnTableEntry
// objects.
aries.TxnStatus = {
  IN_PROGRESS: "in progress",
  COMMITTED:   "committed",
  ABORTED:     "aborted",
}

aries.TxnTableEntry = function(txn_status, last_lsn) {
  this.txn_status = txn_status;
  this.last_lsn = last_lsn;
}

// Dirty Page Table.
//
// The dirty page table records, for each page, the LSN of the oldest log entry
// that dirtied the page. This is dubbed the recLSN of the page. The dirty page
// table is represented by the following types:
//
//   type DirtyPageTableEntry = {
//     rec_lsn: number,
//   }
//
//   type DirtyPageTable = page_id: string -> DirtyPageTableEntry
aries.DirtyPageTableEntry = function(rec_lsn) {
  this.rec_lsn = rec_lsn;
}

// Buffer Pool and Disk.
//
// When pages are written to, they are read from the disk into a cache known as
// the buffer pool. Every page has a corresponding value, and also includes a
// pageLSN: the LSN of the most recent log entry that modified the contents of
// the page. The buffer pool and disk are represented by the following types:
//
//   type Page = {
//     page_lsn: string,
//     value: string,
//   }
//
//   type BufferPool = page_id: string -> Page
//   type Disk = page_id: string -> Page
aries.Page = function(page_lsn, value) {
  this.page_lsn = page_lsn;
  this.value = value;
}

// State.
//
// The state of ARIES is the aggregate of the log, transaction table, dirty
// page table, buffer pool, and disk. The state also includes
//
// - `num_flushed`: the number of log entries that have been flushed to disked.
//
//   type state = {
//     log: aries.Log.Entry list,
//     num_flushed: number,
//     txn_table: TxnTable,
//     dirty_page_table: DirtyPageTable,
//     buffer_pool: BufferPool,
//     disk: Disk,
//   }
aries.State = function(log, num_flushed, txn_table, dirty_page_table,
                       buffer_pool, disk) {
  this.log = log;
  this.num_flushed = num_flushed;
  this.txn_table = txn_table;
  this.dirty_page_table = dirty_page_table;
  this.buffer_pool = buffer_pool;
  this.disk = disk;
}

// Helper Functions ////////////////////////////////////////////////////////////
// `deep_copy(x)` returns a deep copy of `x`. This code is taken from
// http://stackoverflow.com/a/5344074/3187068.
aries.deep_copy = function(x) {
  return JSON.parse(JSON.stringify(x));
}

// `aries.is_object_empty(x)` returns whether the Javascript object `x` is
// empty. See https://goo.gl/KgXgio for implementation.
aries.is_object_empty = function(x) {
  return Object.keys(x).length === 0;
}

// `pages_accessed(ops: Operation list)` returns a list of the page ids of
// every page referenced in ops. The list may contain duplicates. For example,
//
//   var ops = [
//     {type: WRITE,  txn_id:"1", page_id:"A", value:"foo"},
//     {type: WRITE,  txn_id:"2", page_id:"B", value:"bar"},
//     {type: COMMIT, txn_id:"1"                          },
//     {type: FLUSH,              page_id:"B"             }
//   ];
//   aries.pages_accessed(ops) // ["A", "B"]
aries.pages_accessed = function(ops) {
  var page_ids = [];
  for (var i = 0; i < ops.length; i++) {
    if (ops[i].type === aries.Op.Type.WRITE ||
        ops[i].type === aries.Op.Type.FLUSH) {
      page_ids.push(ops[i].page_id);
    }
  }
  return page_ids;
}

// `aries.latest_checkpoint_lsn(state: State)` returns the latest LSN of any
// checkpoint in the log, or 0 if no checkpoints exist.
aries.latest_checkpoint_lsn = function(state) {
  var lsn = 0;
  for (var i = 0; i < state.log.length; i++) {
    if (state.log[i].type === aries.Op.Type.CHECKPOINT) {
      lsn = i;
    }
  }
  return lsn;
}

// `aries.min_rec_lsn(state: State)` returns the minimum recLSN in the dirty
// page table or undefined if the dirty page table is empty.
aries.min_rec_lsn = function(state) {
  var min = undefined;
  for (var page_id in state.dirty_page_table) {
    var rec_lsn = state.dirty_page_table[page_id].rec_lsn;
    if (typeof min === "undefined") {
      min = rec_lsn
    } else {
      min = Math.min(min, rec_lsn);
    }
  }
  return min;
}

// `pin(state: State, page_id: string)` ensures that the page with page id
// `page_id` is pinned in the buffer pool. That is, if the page is already in
// the buffer pool, then this function no-ops. If it isn't, then it is fetched
// from disk.
aries.pin = function(state, page_id) {
  console.assert(page_id in state.disk);
  if (!(page_id in state.buffer_pool)) {
    state.buffer_pool[page_id] = aries.deep_copy(state.disk[page_id]);
  }
}

// `flush(state: State, page_id: string)` is the dual of `aries.pin`; it evicts
// a page from the buffer pool into the disk.
aries.flush = function(state, page_id) {
  console.assert(page_id in state.disk);
  if (page_id in state.buffer_pool) {
    state.disk[page_id] = aries.deep_copy(state.buffer_pool[page_id]);
    delete state.buffer_pool[page_id];
  }
}

// `aries.dirty(state: State, page_id: string, rec_lsn: number)` ensures that
// the page with page id `page_id` is in the dirty page table. If the page is
// already in the dirty page table, then this function no-ops. Otherwise, it
// enters it into the dirty page table with recLSN `rec_lsn`.
aries.dirty = function(state, page_id, rec_lsn) {
  console.assert(page_id in state.disk);
  if (!(page_id in state.dirty_page_table)) {
    state.dirty_page_table[page_id] = new aries.DirtyPageTableEntry(rec_lsn);
  }
}

// `aries.begin_txn(state: State, txn_id: string)` ensures that a transaction
// with transaction id `txn_id` is in the transaction table. If the transaction
// is already in the transaction table, then this no-ops. Otherwise, it enters
// it into the transaction table with undefined status and undefined lastLSN.
aries.begin_txn = function(state, txn_id) {
  if (!(txn_id in state.txn_table)) {
    state.txn_table[txn_id] = new aries.TxnTableEntry(undefined, undefined);
  }
}

// `aries.flush_log(state: State, lsn: number)` ensures the log is flushed up
// to at least `lsn`.
aries.flush_log = function(state, lsn) {
  // When we flush a log entry with LSN `lsn`, there are `lsn + 1` entries
  // flushed. For example, if we flush a single log entry, it has an LSN of 0,
  // but there are 1 entries flushed.
  state.num_flushed = Math.max(state.num_flushed, lsn + 1);
}

// `aries.rec_lsn(state: State, page_id: string)` returns the recLSN of the
// page in the dirty page table, or undefined if its not in the dirty page
// table.
aries.rec_lsn = function(state, page_id) {
  return page_id in state.dirty_page_table ?
    state.dirty_page_table[page_id].rec_lsn :
    undefined;
}

// `aries.last_lsn(state: State, txn_id: string)` returns the transaction id of
// the transaction in the transaction table, or undefined if its not in the
// transaction table.
aries.last_lsn = function(state, txn_id) {
  return txn_id in state.txn_table ?
    state.txn_table[txn_id].last_lsn :
    undefined;
}

// `aries.page_lsn(state: State, page_id: string)` returns the pageLSN of the
// page in the buffer pool, or undefined if its not in the buffer pool. The
// pageLSN is not read from disk if the page is not in the buffer pool.
aries.page_lsn = function(state, page_id) {
  return page_id in state.buffer_pool ?
    state.buffer_pool[page_id].page_lsn :
    undefined;
}

// Init ////////////////////////////////////////////////////////////////////////
// Our ARIES simulator allows operations to write to pages with arbitrary page
// ids. `aries.init(state: State, ops: Operation list)` ensures that all the
// pages referenced in ops are in the disk.
aries.init = function(state, ops) {
  var page_ids = aries.pages_accessed(ops);
  for (var i = 0; i < page_ids.length; i++) {
    state.disk[page_ids[i]] = new aries.Page(-1, "");
  }
}

// Forward Processing //////////////////////////////////////////////////////////
// `aries.process_write(state: State, write: Operation)` processes a write
// operation.
aries.process_write = function(state, write) {
  console.assert(write.type === aries.Op.Type.WRITE);
  var lsn = state.log.length;

  // Bring the page into the buffer pool, if necessary, and update it.
  aries.pin(state, write.page_id);
  var before = state.buffer_pool[write.page_id].value;
  state.buffer_pool[write.page_id].page_lsn = lsn;
  state.buffer_pool[write.page_id].value = write.value;

  // Update the dirty page table, if necessary.
  aries.dirty(state, write.page_id, lsn);

  // Introduce a new transaction into the transaction table, if necessary,
  // and update it.
  var prev_lsn = aries.last_lsn(state, write.txn_id);
  aries.begin_txn(state, write.txn_id);
  state.txn_table[write.txn_id].txn_status = aries.TxnStatus.IN_PROGRESS;
  state.txn_table[write.txn_id].last_lsn = lsn;

  // write update record
  state.log.push(new aries.Log.Update(lsn, write.txn_id, write.page_id,
        before, write.value, prev_lsn));
}

// `aries.process_commit(state: State, commit: Operation)` processes a commit
// operation.
aries.process_commit = function(state, commit) {
  console.assert(commit.type === aries.Op.Type.COMMIT);

  // Write commit and end.
  var commit_lsn = state.log.length;
  var end_lsn = commit_lsn + 1;
  var prev_lsn = aries.last_lsn(state, commit.txn_id)
  state.log.push(new aries.Log.Commit(commit_lsn, commit.txn_id, prev_lsn));
  state.log.push(new aries.Log.End(end_lsn, commit.txn_id, commit_lsn));

  // Flush the log.
  aries.flush_log(state, end_lsn);

  // Clear the transaction from the transaction table.
  delete state.txn_table[commit.txn_id];
}

// `aries.process_checkpoint(state: State, checkpoint: Operation)` processes a
// checkpoint operation.
aries.process_checkpoint = function(state, checkpoint) {
  console.assert(checkpoint.type === aries.Op.Type.CHECKPOINT);
  var lsn = state.log.length;
  state.log.push(new aries.Log.Checkpoint(lsn,
        aries.deep_copy(state.dirty_page_table),
        aries.deep_copy(state.txn_table)));
}

// `aries.process_flush(state: State, flush: Operation)` processes a flush
// operation.
aries.process_flush = function(state, flush) {
  console.assert(flush.type === aries.Op.Type.FLUSH);

  // Flush the log.
  var page_lsn = aries.page_lsn(state, flush.page_id);
  if (typeof page_lsn === "undefined") {
    // If the page isn't in the buffer pool, then it must have already been
    // flushed to disk, so we don't have to do anything.
    console.assert(!(flush.page_id in state.dirty_page_table));
    return;
  }
  aries.flush_log(state, page_lsn);

  // Flush the page to disk.
  aries.flush(state, flush.page_id);

  // Clear the dirty page table.
  delete state.dirty_page_table[flush.page_id];
}

// `aries.forward_process(state: State, ops: Operation list)` processes a
// sequence of operations during normal database operation.
aries.forward_process = function(state, ops) {
  for (var i = 0; i < ops.length; i++) {
    var op = ops[i];
    if (op.type === aries.Op.Type.WRITE) {
      aries.process_write(state, op);
    } else if (op.type === aries.Op.Type.COMMIT) {
      aries.process_commit(state, op);
    } else if (op.type === aries.Op.Type.CHECKPOINT) {
      aries.process_checkpoint(state, op);
    } else if (op.type === aries.Op.Type.FLUSH) {
      aries.process_flush(state, op);
    } else {
      console.assert(false, "Invalid operation type: " + op.type +
                     " in operation " + op);
    }
  }
}

// Crash ///////////////////////////////////////////////////////////////////////
// `aries.crash(state: State)` simulates ARIES crashing by clearing all
// non-ephemeral data.
aries.crash = function(state) {
  state.log = state.log.slice(0, state.num_flushed);
  state.dirty_page_table = {};
  state.txn_table = {};
  state.buffer_pool = {};
}

// Analysis ////////////////////////////////////////////////////////////////////
// `aries.analysis_update(state: State, update: LogEntry)` processes a update
// operation during the analysis phase.
aries.analysis_update = function(state, update) {
  // Update the dirty page table.
  aries.dirty(state, update.page_id, update.lsn);

  // Update the transaction table.
  aries.begin_txn(state, update.txn_id);
  state.txn_table[update.txn_id].txn_status = aries.TxnStatus.ABORTED;
  state.txn_table[update.txn_id].last_lsn = update.lsn;
}

// `aries.analysis_commit(state: State, commit: LogEntry)` processes a commit
// operation during the analysis phase.
aries.analysis_commit = function(state, commit) {
  // Update the transaction table.
  aries.begin_txn(state, commit.txn_id);
  state.txn_table[commit.txn_id].txn_status = aries.TxnStatus.COMMIT;
  state.txn_table[commit.txn_id].last_lsn = commit.lsn;
}

// `aries.analysis_end(state: State, end: LogEntry)` processes an end operation
// during the analysis phase.
aries.analysis_end = function(state, end) {
  // Remove the transaction from the transaction table.
  delete state.txn_table[end.txn_id];
}

// `aries.analysis_clr(state: State, clr: LogEntry)` processes a clr operation
// during the analysis phase.
aries.analysis_clr = function(state, log_entry) {
  console.assert(false, "Our ARIES simulator doesn't support repeated " +
                        "crashes, so the analysis should never see a CLR log " +
                        "entry.");
}

// `aries.analysis_checkpoint(state: State, checkpoint: LogEntry)` processes a
// checkpoint operation during the analysis phase.
aries.analysis_checkpoint = function(state, checkpoint) {
  console.assert(checkpoint.type === aries.Log.Type.CHECKPOINT);

  var state_cleared = aries.is_object_empty(state.dirty_page_table) &&
                      aries.is_object_empty(state.txn_table) &&
                      aries.is_object_empty(state.buffer_pool);
  console.assert(state_cleared, "Analysis should see at most checkpoint. If " +
                                "that checkpoint is encountered, it better be " +
                                "the first thing encountered!");

  state.dirty_page_table = aries.deep_copy(checkpoint.dirty_page_table);
  state.txn_table = aries.deep_copy(checkpoint.txn_table);
}

// `aries.analysis(state: State, ops: Operation list)` simulates the analysis
// phase of ARIES.
aries.analysis = function(state) {
  var start_lsn = aries.latest_checkpoint_lsn(state);
  for (var i = start_lsn; i < state.log.length; i++) {
    var log_entry = state.log[i];
    if (log_entry.type === aries.Log.Type.UPDATE) {
      aries.analysis_update(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.COMMIT) {
      aries.analysis_commit(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.END) {
      aries.analysis_end(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.CLR) {
      aries.analysis_clr(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.CHECKPOINT) {
      aries.analysis_checkpoint(state, log_entry);
    } else {
      console.assert(false, "Invalid log type: " + log_entry.type +
                     " in operation " + log_entry);
    }
  }
}

// Redo ////////////////////////////////////////////////////////////////////////
// `aries.redo_update(state: State, update: LogEntry)` processes a update
// operation during the redo phase.
aries.redo_update = function(state, update) {
  // An update to a page need not be redone if
  //   1. the page is not in the dirty page table;
  //   2. the page is in the dirty page table, and the recLSN is greater than
  //      the LSN of the update; or
  //   3. the recLSN on disk is greater than or equal to the LSN of the update.
  if (!(update.page_id in state.dirty_page_table) ||
      state.dirty_page_table[update.page_id].rec_lsn > update.lsn ||
      state.disk[update.page_id].page_lsn >= update.lsn) {
    return;
  }

  aries.pin(state, update.page_id);
  state.buffer_pool[update.page_id].page_lsn = update.lsn;
  state.buffer_pool[update.page_id].value = update.after;
}

// `aries.redo_commit(state: State, commit: LogEntry)` processes a commit
// operation during the redo phase.
aries.redo_commit = function(state, commit) {
  // Commits are not redone.
}

// `aries.redo_end(state: State, end: LogEntry)` processes an end operation
// during the redo phase.
aries.redo_end = function(state, end) {
  // Ends are not redone.
}

// `aries.redo_clr(state: State, clr: LogEntry)` processes a clr operation
// during the redo phase.
aries.redo_clr = function(state, clr) {
  console.assert(false, "Our ARIES simulator doesn't support repeated " +
                        "crashes, so the redo should never see a CLR log " +
                        "entry.");
}

// `aries.redo_checkpoint(state: State, checkpoint: LogEntry)` processes a
// checkpoint operation during the redo phase.
aries.redo_checkpoint = function(state, checkpoint) {
  // Checkpoints are not redone.
}

// `aries.redo(state: State, ops: Operation list)` simulates the redo phase of
// ARIES.
aries.redo = function(state) {
  var start_lsn = aries.min_rec_lsn(state);
  if (typeof start_lsn === "undefined") {
    // If there are no dirty pages, then we have nothing to redo!
    return;
  }

  for (var i = start_lsn; i < state.log.length; i++) {
    var log_entry = state.log[i];
    if (log_entry.type === aries.Log.Type.UPDATE) {
      aries.redo_update(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.COMMIT) {
      aries.redo_commit(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.END) {
      aries.redo_end(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.CLR) {
      aries.redo_clr(state, log_entry);
    } else if (log_entry.type === aries.Log.Type.CHECKPOINT) {
      aries.redo_checkpoint(state, log_entry);
    } else {
      console.assert(false, "Invalid log type: " + log_entry.type +
                     " in operation " + log_entry);
    }
  }
}

// Undo ////////////////////////////////////////////////////////////////////////
// `aries.undo(state: State, ops: Operation list)` simulates the undo phase of
// ARIES.
aries.undo = function(state) {
  var losers = [];
  for (var page_id in state.txn_table) {
    losers.push(state.txn_table[page_id].last_lsn);
  }

  while (losers.length > 0) {
    // Get the loser log entry. We repeatedly sort the loser transaction LSNs
    // and pop the last (i.e. biggest) LSN.
    losers.sort();
    var loser = losers.pop();
    var loser_entry = state.log[loser];
    console.assert(loser_entry.type === aries.Log.Type.UPDATE,
        "Our ARIES simulator doesn't support repeated crashes, so the undo " +
        "phase should never see a CLR log entry.");

    // Append a CLR entry.
    var clr_lsn = state.log.length;
    var undo_next_lsn = loser_entry.prev_lsn;
    var after = loser_entry.before;
    var prev_lsn = aries.last_lsn(state, loser_entry.txn_id);
    state.log.push(new aries.Log.CLR(clr_lsn, loser_entry.txn_ind,
          loser_entry.page_id, undo_next_lsn, after, prev_lsn));

    if (typeof undo_next_lsn !== "undefined") {
      // Update the loser transactions.
      losers.push(undo_next_lsn);
    } else {
      // End a completely undone transaction and remove it from the transaction
      // table.
      state.log.push(new aries.Log.End(state.log.length, loser_entry.txn_id,
                                   clr_lsn));
      delete state.txn_table[loser_entry.txn_id];
    }
  }
}

// Main ////////////////////////////////////////////////////////////////////////
// `aries.simulate(ops: Operation list)` Simulate the execution of ARIES on
// `ops`.
aries.simulate = function(ops) {
  var log = [];
  var num_flushed = 0;
  var txn_table = {};
  var dirty_page_table = {};
  var buffer_pool = {};
  var disk = {};
  var state = new aries.State(log, num_flushed, txn_table, dirty_page_table,
                              buffer_pool, disk);

  aries.init(state, ops);
  aries.forward_process(state, ops);
  aries.crash(state);
  aries.analysis(state);
  aries.redo(state);
  aries.undo(state);
}

function main() {
  // TODO(mwhittaker): Parse operations from the user.
  var ops = [
    new aries.Op.Write("1", "A", "foo"),
    new aries.Op.Write("2", "B", "foo"),
    new aries.Op.Flush("B"),
    new aries.Op.Write("3", "C", "foo"),
    new aries.Op.Flush("C"),
    new aries.Op.Checkpoint(),
    new aries.Op.Write("2", "D", "foo"),
    new aries.Op.Write("1", "A", "foo"),
    new aries.Op.Commit("1"),
    new aries.Op.Write("3", "C", "foo"),
    new aries.Op.Write("2", "D", "foo"),
    new aries.Op.Flush("D"),
    new aries.Op.Write("2", "B", "foo"),
    new aries.Op.Write("3", "A", "foo"),
  ];
  aries.simulate(ops);
}

main();
