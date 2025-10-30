# Contents of this repo

---

- Scala and Spark recap
- Performance foundations
  - reading the Spark UI
  - understanding transformations
  - reading DAGs and query plans
- Optimising DataFrame joins
- Optimising RDD joins
- Optimising key-value RDDs

---

## Optimisation Goals

- Optimise the time it takes for a job to run
  - understanding how spark works internally
  - writing efficient code

Important to do this first. Write good code and then add resources and money at the problem.

- Will not optimise (see other repo)
  - memory usage
  - cluster resource (CPU, mem, bandwidth) usage
  - compute time via configs

*write good code first* - hard to squeeze performance out of bad code

---

## Optimisation Skills covered

- foundational skills
- deconstruction
- selection
- sequencing

