# Notes

- Page does not have info for the corresponding block ID (BlockId), 
  however, buffer has the info for the corresponding block ID.
- FileManager is the bridge between Page (memory) & BlockId (disk)
- FileManager has a cache for open file handlers, which is used to avoid opening the same file handler multiple times.