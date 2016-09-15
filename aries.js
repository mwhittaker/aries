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
// TODO(larry): Is this how namespacing in Javascript should work? -michael
aries = {};

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
//     | CHECKPOINT
//     | FLUSH
//
//   type Op.Operation = {
//     type:   aries.Op.Type,
//     txn_id: string,
//     args:   string list
//   }
//
//   | command    | txn_id | args                             |
//   | -------    | ------ | -------------------------------- |
//   | write      | y      | {page_id: string, value: string} |
//   | commit     | y      | {}                               |
//   | checkpoint | y      | {}                               |
//   | flush      | n      | {page_id: string}                |
//
// For example, the sequence of operations above would be represented as:
//
//   - {type: WRITE,  txn_id:"1", args:{page_id:"A", value:"foo"}}
//   - {type: WRITE,  txn_id:"2", args:{page_id:"B", value:"bar"}}
//   - {type: COMMIT, txn_id:"1", args:{}}
//   - {type: FLUSH,              args:{page_id: string}}
aries.Op = {};
aries.Op.Type = {
  WRITE:      "write",
  COMMIT:     "commit",
  CHECKPOINT: "checkpoint",
  FLUSH:      "flush",
}

aries.Op.Operation = function(type, txn_id, args) {
  this.type = type;
  this.txn_id = txn_id;
  this.args = args;
}

aries.Op.Write = function(txn_id, page_id, value) {
  return new aries.Op.Operation(aries.Op.Type.WRITE, txn_id, {
    page_id: page_id,
    value: value,
  });
}

aries.Op.Commit = function(txn_id) {
  return new aries.Op.Operation(aries.Op.Type.COMMIT, txn_id, {});
}

aries.Op.Checkpoint = function(txn_id) {
  return new aries.Op.Operation(aries.Op.Type.CHECKPOINT, txn_id, {});
}

aries.Op.Flush = function(page_id) {
  return new aries.Op.Operation(aries.Op.Type.FLUSH, undefined, {
    page_id: page_id,
  });
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
//   type LogEntry = {
//     lsn:              number,
//     type:             aries.LogTypes,
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
//   type Log = LogEntry list
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
aries.LogType = {
  UPDATE:     "update",
  COMMIT:     "commit",
  END:        "end",
  CLR:        "clr",
  CHECKPOINT: "checkpoint",
}

aries.Update = function(lsn, txn_id, page_id, before, after, prev_lsn) {
  this.lsn = lsn;
  this.type = aries.LogType.UPDATE;
  this.txn_id = txn_id;
  this.page_id = page_id;
  this.before = before;
  this.after = after;
  this.prev_lsn = prev_lsn;
}

aries.Commit = function(lsn, txn_id, prev_lsn) {
  this.lsn = lsn;
  this.type = aries.LogType.COMMIT;
  this.txn_id = txn_id;
  this.prev_lsn = prev_lsn;
}

aries.End = function(lsn, txn_id, prev_lsn) {
  this.lsn = lsn;
  this.type = aries.LogType.END;
  this.txn_id = txn_id;
  this.prev_lsn = prev_lsn;
}

aries.Clr = function(lsn, txn_id, page_id, after, undo_next_lsn, prev_lsn) {
  this.lsn = lsn;
  this.type = aries.LogType.CLR;
  this.txn_id = txn_id;
  this.page_id = page_id;
  this.after = after;
  this.undo_next_lsn = undo_next_lsn;
  this.prev_lsn = prev_lsn;
}

aries.Checkpoint = function(lsn, dirty_page_table, txn_table) {
  this.lsn = lsn;
  this.type = aries.LogType.CHECKPOINT;
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
//     log: Log,
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
//   var op1 = {type: WRITE,  txn_id:"1", args:{page_id:"A", value:"foo"}};
//   var op2 = {type: WRITE,  txn_id:"2", args:{page_id:"B", value:"bar"}};
//   var op3 = {type: COMMIT, txn_id:"1", args:{}};
//   var op4 = {type: FLUSH,              args:{page_id: string}};
//   var ops = [op1, op2, op3, op4];
//   aries.pages_accessed(ops) // ["A", "B"]
aries.pages_accessed = function(ops) {
  var page_ids = [];
  for (var i = 0; i < ops.length; i++) {
    if (ops[i].type === aries.Op.Type.WRITE ||
        ops[i].type === aries.Op.Type.FLUSH) {
      page_ids.push(ops[i].args.page_id);
    }
  }
  return page_ids;
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
  aries.pin(state, write.args.page_id);
  var before = state.buffer_pool[write.args.page_id].value;
  state.buffer_pool[write.args.page_id].page_lsn = lsn;
  state.buffer_pool[write.args.page_id].value = write.args.value;

  // Update the dirty page table, if necessary.
  aries.dirty(state, write.args.page_id, lsn);

  // Introduce a new transaction into the transaction table, if necessary,
  // and update it.
  var prev_lsn = aries.last_lsn(state, write.txn_id);
  aries.begin_txn(state, write.txn_id);
  state.txn_table[write.txn_id].txn_status = aries.TxnStatus.IN_PROGRESS;
  state.txn_table[write.txn_id].last_lsn = lsn;

  // write update record
  state.log.push(new aries.Update(lsn, write.txn_id, write.args.page_id,
        before, write.args.value, prev_lsn));
}

// `aries.process_commit(state: State, commit: Operation)` processes a commit
// operation.
aries.process_commit = function(state, commit) {
  console.assert(commit.type === aries.Op.Type.COMMIT);

  // Write commit and end.
  var commit_lsn = state.log.length;
  var end_lsn = commit_lsn + 1;
  var prev_lsn = aries.last_lsn(state, commit.txn_id)
  state.log.push(new aries.Commit(commit_lsn, commit.txn_id, prev_lsn));
  state.log.push(new aries.End(end_lsn, commit.txn_id, commit_lsn));

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
  state.log.push(new aries.Checkpoint(lsn,
        aries.deep_copy(state.dirty_page_table),
        aries.deep_copy(state.txn_table)));
}

// `aries.process_flush(state: State, flush: Operation)` processes a flush
// operation.
aries.process_flush = function(state, flush) {
  console.assert(flush.type === aries.Op.Type.FLUSH);

  // Flush the log.
  var page_lsn = aries.page_lsn(state, flush.args.page_id);
  if (typeof page_lsn === "undefined") {
    // If the page isn't in the buffer pool, then it must have already been
    // flushed to disk, so we don't have to do anything.
    console.assert(!(flush.args.page_id in state.dirty_page_table));
    return;
  }
  aries.flush_log(state, page_lsn);

  // Flush the page to disk.
  aries.flush(state, flush.args.page_id);

  // Clear the dirty page table.
  delete state.dirty_page_table[flush.args.page_id];
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
      console.log("Invalid operation type: " + op.type + " in operation " + op);
    }
    console.log(state);
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
  console.assert(checkpoint.type === aries.LogType.CHECKPOINT);

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
aries.analysis = function(state, ops) {
  var start_lsn = aries.latest_checkpoint_lsn(state);
  for (var i = start_lsn; i < state.log.length; i++) {
    var log_entry = state.log[i];
    if (log_entry.type === aries.LogType.UPDATE) {
      aries.analysis_update(state, log_entry);
    } else if (log_entry.type === aries.LogType.COMMIT) {
      aries.analysis_commit(state, log_entry);
    } else if (log_entry.type === aries.LogType.END) {
      aries.analysis_end(state, log_entry);
    } else if (log_entry.type === aries.LogType.CLR) {
      aries.analysis_clr(state, log_entry);
    } else if (log_entry.type === aries.LogType.CHECKPOINT) {
      aries.analysis_checkpoint(state, log_entry);
    } else {
      console.log("Invalid operation type: " + op.type + " in operation " + op);
    }
    console.log(state);
  }
}

// Redo ////////////////////////////////////////////////////////////////////////
// `aries.redo(state: State, ops: Operation list)` simulates the redo phase of
// ARIES.
aries.redo = function(state, ops) {
  // TODO(mwhittaker): Implement.
}

// Undo ////////////////////////////////////////////////////////////////////////
// `aries.undo(state: State, ops: Operation list)` simulates the undo phase of
// ARIES.
aries.undo = function(state, ops) {
  // TODO(mwhittaker): Implement.
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
  aries.analysis(state, ops);
  aries.redo(state, ops);
  aries.undo(state, ops);
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
