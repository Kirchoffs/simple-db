# simple-db
Reference: `Database Design and Implementation` by Edward Sciore

## Database Notes
### Buffer Page & Log Page
- Buffer page: a page in the buffer pool, managed by BufferManager, which is used for data in memory.
- Log page: a page managed by LogManager, which is used for log record.

### WAL
Write-Ahead Logging: flushing an update log record to disk before flushing the corresponding modified buffer page.

The standard way to implement a write-ahead log is for each buffer to keep track of the LSN of its most recent modification. 
Before a buffer replaces a modified page (which means the modified page should be flush to disk), it tells the log manager to flush the log up to that LSN. 
As a result, the log record corresponding to a modification will always be on disk before the modification gets saved to disk.

## Java Notes
### ByteBuffer
> java.nio.ByteBuffer

Byte buffers can be created either by allocation, which allocates space for the buffer's content, or by wrapping an existing byte array into a buffer.

### ByteBuf
> io.netty.buffer.ByteBuf

### RandomAccessFile
> java.util.RandomAccessFile

#### Write
Write from ByteBuffer:
```
RandomAccessFile f = getFile(blockId.getFilename());
f.seek(blockId.getBlockNum() * blockSize);
f.getChannel().write(page.contents());
```

Write from byte array:
```
byte[] bytes = new byte[blockSize];
RandomAccessFile f = getFile(filename);
f.seek(newBlockNum * blockSize);
f.write(bytes);
```

Difference between `f.getChannel().write(byteBuffer)` and `f.write(byteArray)`:
- ByteBuffer is a more versatile and flexible way to work with binary data.
  You can easily manipulate the data in the ByteBuffer before writing it to the file.
  This method is often used when you need precise control over the data you're writing.

- f.write(byteArray) is more suitable for cases where you have a byte array that contains the exact data you want to write,
  and you don't need to perform additional manipulation on the data before writing it.

### File
> java.io.File

- Method: String[] list()

Returns an array of strings naming the files and directories in the directory denoted by this abstract pathname.

