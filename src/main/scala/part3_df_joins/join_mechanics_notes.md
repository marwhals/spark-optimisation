 # General join mechanics notes
 
Recap of joins.

---

## Joins - *in this context a table is a DataFrame or a RDD*

- Combine the data in multiple DataFrames/RDDs
  - Rows are combined
  - join condition: only the rows passing the condition are kept
- Join Types
  - inner: combine only the rows passing the condition
  - left_outer: inner + all rows in the left "table", with nulls in the corresponding fields of the other table
  - right_out: same for the right "table"
  - full_outer = same for both "tables"
- More join types (DFs only)
  - left_semi - all the rows in the left DF for which there is a row in the right DF passing the condition
  - left_anti - all the rows in the left DF for which there is NO row in the right DF passing the condition

---

## Why are joins slow?

- If DFs or RDDs don't have a known partitioner, a shuffle is needed
  - data transfer overhead
  - potential OOMs
  - limited parallelism
- Co-located RDDs
  - have the same partitioner
  - reside in the same physical location in memory (on the same executor)
  - can be joined without any network transfer
- Co-paritioned RDDs
  - have the same partitioner
  - may be on different executors
  - will (in general) be joined with network traffic
    - although much less than without the partitioning information
- Shuffled Join
  - No partitioner is known
    - Rows with the same key must be on the same partition
    - Spark needs to shuffle both RDDs
    - VERY expensive
    - 
[//]: # (  - TODO add diagram)

- Optimised Join
  - One RDD doesn't have a known partitioner, or partitioners are different
    - we can force the other RDD to obey the same partitioner
    - one shuffle instead of two

[//]: # (    - TODO add diagram)

- Optimised Join +
- Both RDDs have the same partitioner (co-partitioned)
  - Just fetch the existing partitions and do the join
  - No shuffles
  - Narrow dependency

[//]: # (TODO add diagram)

- Optimised Join ++
- Same partitoners, partitions loaded in memory (co-located
  - No partition fetching
  - No shuffle
  - No network transfer
  - Fastest join possible

[//]: # (TODO add diagram)

- Join mechanics
  - Shuffling, colocation and co partitioning apply to RDDs and DFs
  - Techniques apply to grouping as well