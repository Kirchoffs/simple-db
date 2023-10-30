# Notes

## Database Related
### Log Management Algorithm
Permanently allocate one memory page to hold the contents of the last block of the log file. Call this page P.

When a new log record is submitted:
- If there is no room in P, then write P to disk and clear its contents.
- Append the new log record to P.

When the database system requests that a particular log record be written to disk:
- Determine if that log record is in P, if so, then write P to disk.

### Log Page / Block Format
The first integer of the page represents the position values for the upcoming log entry to be appended. 
It's initialized with the page size value. For instance, if the page size is 100 bytes, then the first integer is set to 100.

As logs are appended, the position value moves backwards. Imagine appending a new log entry of 10 bytes. 
This entry would be stored in positions 90 to 100, causing the initial integer value of the page to be updated to 90.
