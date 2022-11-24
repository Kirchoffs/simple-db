# Notes
A materialized implementation of an operator preprocesses its underlying records, storing them in one or more temporary tables. 
Its scan methods are thus more efficient, because they only need to examine the temporary tables.

The materialize operator creates a temporary table containing all of its input records. 
It is useful whenever its input is executed repeatedly, such as when it is on the right side of a product node.
