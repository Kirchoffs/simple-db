# Notes
## LockTable
It is responsible for granting and releasing locks for transaction.

## Transaction
Each transaction has its own concurrency manager and recovery manager. The method of concurrency manager are similar to those of the lock table
but are transaction-specific.  

All transactions share a single lock table.
