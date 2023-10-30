# Notes

## Use Case
When a client asks the buffer manager to pin a page to a block, the buffer manager will encounter one of these four possibilities:

The contents of the block is in some page in the buffer, and:
- The page is pinned.
- The page is unpinned.

The contents of the block is not currently in any buffer, and:
- There exists at least one unpinned page in the buffer pool.
- All pages in the buffer pool are pinned.

## Pin
A page is said to be pinned if some client is currently pinning it; otherwise, the page is unpinned. 
The buffer manager is obligated to keep a page available to its clients for as long as it is pinned.

## Buffer Replacement Strategies
The pages in the buffer pool begin unallocated. 
As pin requests arrive, the buffer manager primes the buffer pool by assigning requested blocks to unallocated pages. 
Once all pages have been allocated, the buffer manager will begin replacing pages. The buffer manager may choose any unpinned page in the buffer pool for replacement.

- Naive
- FIFO  
  Choosing the buffer that was __least recently replaced__, that is, the page that has been sitting in the buffer pool the longest.
- LRU  
  The FIFO strategy bases its replacement decision on when a page was added to the buffer pool. 
  A similar strategy would be to make the decision based on when a page was __least recently used__, 
  the rationale being that a page that has not been used in the near past will also not be used in the near future.
  The __time unpinned__ corresponds to when the buffer was last used.
- Clock  
  As in the naive strategy, the clock replacement algorithm scans through the buffer pool, choosing the first unpinned page it finds. 
  The difference is that the algorithm always starts its scan at the page after the previous replacement. 
  If you visualize the buffer pool as forming a circle, then the replacement algorithm scans the pool like the hand of an analog clock, 
  stopping when a page is replaced and starting when another replacement is required.

## Block & Page & Buffer
Block -> Buffer

The underlying data of Buffer is stored on the corresponding Page.
