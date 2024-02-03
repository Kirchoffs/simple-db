# Notes

- Page does not have info for the corresponding block ID (BlockId), 
  however, buffer has the info for the corresponding block ID.

- FileManager is the bridge between Page (memory) & BlockId (disk)

- FileManager has a cache for open file handlers, which is used to avoid opening the same file handler multiple times.

- `ByteBuffer` is a class provided in the java.nio package. It represents a buffer, a linear, fixed-size container for bytes, 
which can be used for reading from or writing to channels, among other things. ByteBuffer provides methods for reading and writing different data types, 
managing the buffer's position, limit, and capacity, and performing various operations on the data stored within it.

- In Netty, data received from the network is typically read into a `ByteBuf` rather than a ByteBuffer. 
ByteBuf is a more flexible and efficient buffer implementation provided by Netty that extends the functionality of ByteBuffer. 
It provides more advanced features such as reference counting, allowing for efficient memory management, and more flexible slicing and pooling mechanisms. 
While ByteBuffer and ByteBuf serve similar purposes, ByteBuf offers additional features and optimizations tailored for network applications, 
making it a preferred choice when working with network-related data in Netty.
