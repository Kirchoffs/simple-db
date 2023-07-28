# Notes

## Use Case
When a client asks the buffer manager to pin a page to a block, the buffer manager will encounter one of these four possibilities:

The contents of the block is in some page in the buffer, and:
- The page is pinned.
- The page is unpinned.

The contents of the block is not currently in any buffer, and:
- There exists at least one unpinned page in the buffer pool.
- All pages in the buffer pool are pinned.