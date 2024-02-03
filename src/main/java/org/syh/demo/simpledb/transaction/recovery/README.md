# Notes
The recovery manager is responsible for ensuring atomicity and durability. It is the portion of the server that reads and processes the log.  

It has three functions: to write log records, to __roll back__ a transaction, and to __recover the database__ after a
system crash.