# simple-db
Reference: `Database Design and Implementation` by Edward Sciore

## Database Notes
### Buffer Page & Log Page
- Buffer page: a page in the buffer pool, managed by BufferManager, which is used for data in memory.
- Log page: a page managed by LogManager, which is used for log record.

Buffer manager is responsible for buffer page and data files, while log manager is responsible for log page and log files.
They are both based on FileManager.

### WAL
Write-Ahead Logging: flushing an update log record to disk before flushing the corresponding modified buffer page.

The standard way to implement a write-ahead log is for each buffer to keep track of the LSN of its most recent modification. 
Before a buffer replaces a modified page (which means the modified page should be flush to disk), it tells the log manager to flush the log up to that LSN. 
As a result, the log record corresponding to a modification will always be on disk before the modification gets saved to disk.

### JDBC
#### Connection String
```
jdbc:mysql://localhost:3306/simple-db
jdbc:derby://localhost:1527/simple-db;create=true
jdbc:simple-db://localhost
```

#### General
```
String url = "jdbc:derby://localhost/test-db;create=true"; 
Driver driver = new ClientDriver();
Connection conn = driver.connect(url, null);
```

```
String url = "jdbc:derby://localhost/test-db"; 
Properties prop = new Properties(); 
prop.put("create", "true"); 
prop.put("username", "einstein"); 
prop.put("password", "emc2");
Driver driver = new ClientDriver(); 
Connection conn = driver.connect(url, prop);
```

```
String url = "jdbc:simple-db://localhost"; 
Driver driver = new NetworkDriver();
Connection conn = d.connect(url, null);
```

```
try (Connection conn = d.connect(url, null)) { 
    System.out.println("Database Created");
} catch (SQLException e) {
    e.printStackTrace();
}
```

#### DriverManager
```
DriverManager.registerDriver(new ClientDriver()); 
DriverManager.registerDriver(new NetworkDriver()); 

String url = "jdbc:derby://localhost/studentdb"; 
Connection conn = DriverManager.getConnection(url);
```

Or, put it in property file:
```
jdbc.drivers=
org.apache.derby.jdbc.ClientDriver:simpledb.remote.NetworkDriver
```

#### DataSource
For Derby, you need to use `org.apache.derby.jdbc.ClientDataSource` or `org.apache.derby.jdbc.EmbeddedDataSource`.
```
ClientDataSource ds = new ClientDataSource(); 
ds.setServerName("localhost"); 
ds.setDatabaseName("studentdb");
Connection conn = ds.getConnection();
```

## Java Notes
### ByteBuffer
> java.nio.ByteBuffer

Byte buffers can be created either by allocation, which allocates space for the buffer's content, or by wrapping an existing byte array into a buffer.

### ByteBuf
> io.netty.buffer.ByteBuf

### RandomAccessFile
> java.util.RandomAccessFile

Instances of this class support both reading and writing to a random access file. A random access file behaves like a large array of bytes stored in the file system. 
There is a kind of cursor, or index into the implied array, called the file pointer; input operations read bytes starting at the file pointer and advance the file pointer past the bytes read. 
If the random access file is created in read/write mode, then output operations are also available; output operations write bytes starting at the file pointer and advance the file pointer past the bytes written.
Output operations that write past the current end of the implied array cause the array to be extended. 
The file pointer can be read by the `getFilePointer` method and set by the `seek` method.

#### FileChannel
```
RandomAccessFile f = getFile(blockId.getFilename());
f.getChannel().read(byteBuffer);
```

A channel for reading, writing, mapping, and manipulating a file.
A file channel is a `SeekableByteChannel` that is connected to a file. It has a current position within its file which can be both queried and modified. 

The file itself contains a variable-length sequence of bytes that can be read and written and whose current size can be queried. 
The size of the file increases when bytes are written beyond its current size; the size of the file decreases when it is truncated. 
The file may also have some associated metadata such as access permissions, content type, and last-modification time; `FileChannel` does not define methods for metadata access.

In addition to the familiar read, write, and close operations of byte channels, `FileChannel` defines the following file-specific operations:

- Bytes may be read or written at an absolute position in a file in a way that does not affect the channel's current position. (use `read(ByteBuffer dst, long position)`)
- A region of a file may be mapped directly into memory; for large files this is often much more efficient than invoking the usual read or write methods.
- Updates made to a file may be forced out to the underlying storage device, ensuring that data are not lost in the event of a system crash.
- Bytes can be transferred from a file to some other channel, and vice versa, in a way that can be optimized by many operating systems into a very fast transfer directly to or from the filesystem cache.
- A region of a file may be locked against access by other programs.
- File channels are safe for use by multiple concurrent threads.
- Changing the channel's position, whether explicitly or by reading or writing bytes, will change the file position of the originating object, and vice versa. 
- Changing the file's length via the file channel will change the length seen via the originating object, and vice versa.

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

### String and Encoding
#### Get the number of bytes of a string
```
String str = "Hello World";
byte[] bytes = str.getBytes();
int numBytes = bytes.length;
```

```
import java.nio.charset.StandardCharsets;

String str = "Hello World";
byte[] bytes = str.getBytes(StandardCharsets.US_ASCII);
int numBytes = bytes.length;
```
