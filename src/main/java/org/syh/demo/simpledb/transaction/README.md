# Notes

## Log Record
### Types
There are four basic kinds of log records: start records, commit records, rollback records, and update records.

In the code, it is: START, COMMIT, ROLLBACK, SETINT, SETSTRING

### Format
```
SETINT <transaction_num> <file_name> <block_num> <offset> <old_value> <new_value>
```

## Recovery
### Operations
- Flush the update to disk
- Flush the update log record to disk
- Flush the commit log record to disk

The order of these operations is important. Based on different recovery algorithms, the order may vary. 
But flush the update log record to disk before the update is a common rule.


Besides, there are three more operations:
- Write the update to the data buffer
- Write the update log record to the log buffer
- Write the commit log record to the log buffer

### Undo-Redo Algorithm
Stage 1: read backward from the end and undo all the transactions that did not commit.  
Stage 2: read forward from the beginning and redo all the transactions that did commit.

- Undo is for uncompleted transactions in which some log records are not committed.
- Redo is for the completed transactions that do not have their modifications written to disk.

The flush-to-disk order is:
1. Flush the update log record to disk.
2. The order of the next two steps (flush the update to disk and flush the commit log record to disk) is not important.

Side notes:   
- __committed (transaction completed) != data written to disk__  
We have two kinds of data in the memory: data buffer and log buffer. 
If we are using WAL, the update log record should be flushed to disk before the corresponding modified buffer page is flushed to disk.

### Undo-Only Algorithm
Stage 2 can be omitted if the recovery manager is sure that all committed modifications have been written to disk.

The recovery manager can do so by forcing the buffers to disk before it writes the commit log record to the disk.

The flush-to-disk order is:
1. Flush the update log record to disk.
2. Flush the update to disk.
3. Flush the commit log record to disk.

The log is also a bit smaller, because update records no longer need to contain the new modified value.

The original __SimpleDB recovery manager__ uses __undo-only__ algorithm, and the log record format is:
```
SETINT <transaction_num> <file_name> <block_num> <offset> <old_value>
```

### Redo-Only Algorithm
Stage 1 can be omitted if uncommitted buffers are never written to disk. 

The recovery manager can ensure this property by having each transaction keep its buffers pinned until the transaction completes.

The flush-to-disk order is:
1. Flush the update log record to disk.
2. Flush the commit log record to disk.
3. Flush the update to disk.

### Quiescent Checkpointing
1. Stop accepting new transactions.
2. Wait for existing transactions to finish.
3. Flush all modified buffers.
4. Append a quiescent checkpoint record <CHECKPOINT> to the log and flush it to disk. 
5. Start accepting new transactions.

A good time to write a quiescent checkpoint record is during system startup, after recovery has completed and before new transactions have begun. 
Since the recovery algorithm has just finished processing the log, the checkpoint record ensures that it will never need to examine those log records again.

### Non-quiescent Checkpointing
#### Description 1
1. Let T1, T2, ... Tk be the currently running transactions. 
2. Stop accepting new transactions.
3. Flush all modified buffers.
4. Write the record <NQCKPT T1 T2 ... Tk> into the log. 
5. Start accepting new transactions.

#### Description 2
1. Let T1, T2, ... Tk be the currently running transactions.
2. Write the record <START NQCKPT T1 T2 ... Tk> into the log.
3. Wait until all transactions T1, T2, ... Tk have committed, but not prohibit new transactions from starting.
4. When all transactions T1, T2, ... Tk have committed, write the log record <END NQCKPT T1 T2 ... Tk> and flush it to disk.

#### Undo-Only using Non-quiescent Checkpointing Description 2
If we first meet <END NQCKPT T1 T2 ... Tk>, we know all incomplete transactions began after the previous <START NQCKPT T1 T2 ... Tk> record.
So, we can scan backwards as far as the next <START NQCKPT T1 T2 ... Tk> record and undo all the transactions that did not commit.

If we first meet <START NQCKPT T1 T2 ... Tk>, then the incomplete transactions are __those__ we met scanning backwards before we reached 
the <START NOCKPT> record and __those__ of T1, T2, ... Tk that did not complete before the crash.

Optimization: we can chain all the log records of the same transaction together, so we can easily find all the log records of a transaction.

#### Key
As a general rule, once an <END NQCKPT> record has been written to disk, we can delete the log prior to the previous <START NQCKPT> record.

## Concurrency
### Schedule
#### History
The history of a transaction is its sequence of calls to methods that access the database files — in particular, the get/set methods.  
Formally, the history of a transaction is the sequence of database actions made by that transaction.

#### Schedule
When multiple transactions are running concurrently, the database engine will interleave the execution of their threads, periodically interrupting one thread and resuming another.
Thus, the actual sequence of operations performed by the concurrency manager will be an unpredictable interleaving of the histories of its transactions. That interleaving is called a schedule.

#### Serial Schedule
The operations in this schedule will not be interleaved, that is, the schedule will simply be the back-to-back histories of each transaction. This kind of schedule is called a serial schedule.

#### Non-serial Schedule
The operations in this schedule will be interleaved.

#### Serializable Schedule
A non-serial schedule is said to be serializable if it produces the same result as some serial schedule.

#### Non-serializable Schedule
The result of a schedule cannot be produced by any serial schedule, then this kind of schedule is said to be non-serializable.

#### Correct Schedule
Recall the ACID property of isolation, which said that each transaction should execute as if it were the only transaction in the system. A non-serializable schedule does not have this property. 
Therefore, you are forced to admit that non-serializable schedules are incorrect. In other words, a schedule is correct if and only if it is serializable.
The database engine is responsible for ensuring that all schedules are serializable.

### Conflict
- write-write conflict
- read-write conflict

The reason to care about conflicts is that they influence the serializability of a schedule. 
The order in which conflicting operations are executed in a non-serial schedule determines what the equivalent serial schedule must be.

Locking can be used to avoid write-write and read-write conflicts.

### Lock
The database engine is responsible for ensuring that all schedules are serializable. 
A common technique is to use locking to postpone the execution of a transaction.

#### Lock Protocol
- Before reading a block, acquire a shared lock on it.
- Before modifying a block, acquire an exclusive lock on it.
- Release all locks after a commit or rollback.

If all the transactions follow the protocol, then:
- The resulting schedule will always be serializable (and hence correct).
- The equivalent serial schedule is determined by the order in which the transactions commit.

If a transaction acquires all of its locks before unlocking any of them, the resulting schedule is guaranteed to be serializable.
This variant of the lock protocol is called two-phase locking. This name comes from the fact that under this protocol, 
a transaction has two phases—the phase where it accumulates the locks and the phase where it releases the locks.

#### Deadlock
Solution:
- Wait-for graph
- Wait-die (approximate solution)  
  Suppose T1 requests a lock that T2 holds:  
  If T1 is older than T2, then T1 waits.  
  If T1 is younger than T2, then T1 aborts.
- Time limit deadlock detection (approximate solution)

### Multiversion Locking
- Each version of a block is timestamped with the commit time of the transaction that wrote it.
- When a read-only transaction requests a value from a block, the concurrency manager uses the version of the block that was most recently committed at the time the transaction began.

Multiversion locking allows read-only transactions to execute without locks and therefore without the inconvenience of having to wait.

### Transaction Level
- Serializable
- Repeatable Read
- Read Committed
- Read Uncommitted

All transactions, regardless of their isolation level, should behave correctly with respect to writing data. 
They must obtain the appropriate xlocks (including the xlock on the eof marker) and hold them to completion.

## Transaction in Project
### How to read an integer value from a block
1. ConcurrencyManager acquires a slock on the block.
2. Get the buffer from BufferManager.
3. BufferManager returns the value from the buffer based on the offset.

### How to write an integer value to a block
1. In order to get the xlock, ConcurrencyManager acquires a slock on the block.
2. ConcurrencyManager acquires a xlock on the block.
3. Get the buffer from BufferManager.
4. RecoveryManager appends a SETINT record to the log.

### How to get the size of the data file
1. Build a dummy block with id -1.
2. ConcurrencyManager acquires a slock on the block.
3. FileManager returns the size of the data file.

### How to commit
1. BufferManager flushes all the buffers with the corresponding transaction number to disk. 
2. LogManager appends a commit record to the log and flushes it to disk (based on lsn).
3. ConcurrencyManager releases all the locks held by the transaction.
4. BufferManager unpins all the buffers held by the transaction.
