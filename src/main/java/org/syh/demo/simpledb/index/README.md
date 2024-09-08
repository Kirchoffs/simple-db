# Notes
## Glossary
- BTreeDir -> Directory block -> Directory record
- BTreeLeaf -> Leaf block (Index block) -> Leaf record (Index record)

## Structure
Level n directory block (contains directory record) ->  
level (n - 1) directory block (contains directory record) -> ... ->  
level 0 directory block (contains directory record) ->  
Index block (contains index record)

## Directory & Index
- Level-0 directory will have a record ([dataVal, blockNum]) for each block of the index file, where _dataVal_ is the dataVal
of the first record in the block and _blockNum_ is the block number of that block.

- In general, the dataVal in the first directory record is not interesting and is usually replaced by a special value (such as null), denoting "everything from the beginning".
