# Optimising RDD transformations

---

- you don't get the same control for the same operations in DataFrames

---

# Dependencies

- Narrow Dependencies
  - one input (parent) partition influences a single output (child) partition
  - fast to compute
  - Example: map, flatMap, filter, mapValues
  - ADD DIAGRAM
- Wide dependencies
  - One input partition influences more than one output partition
  - Involve a shuffle i.e data transfer between Spark executors
  - These are costly to compute
  - Examples: grouping, joining and sorting
  - ADD DIAGRAM

---

# Performance Consequences

- Narrow dependencies are fast
  - No data transfer between executors
  - Can be executed in a single pass over data
  - Fault-tolerance: if an executor fails, recomputing a partition needs a single parent partition
- Wide dependencies are bad for multiple reasons
  - Need data transfer between executors - slow!
  - May need disk IO for shuffle files
  - Impose stage boundaries
  - Limit parallelism
  - Fault-tolerance: If an executor fails, recomputing a partition can take a very long time.

---

# RDD Implementations

- RDD variations are different in two ways
  - in the type of elements contained
  - in the actual implementation of the RDD interface
- RDDs have different APIs
  - some operations are only available for RDDs of tuples
- RDD transformations can be computed differently
  - certain RDD implementations may hold additional information e.g. locality/ordering
  - Example: MappedRDD vs GroupedRDD
