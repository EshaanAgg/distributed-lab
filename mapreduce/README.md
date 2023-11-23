# Map Reduce

This is the implementation of the [`Map-Reduce`](http://nil.csail.mit.edu/6.824/2017/papers/mapreduce.pdf) algorithm for counting words in files.

## Implementation Details

### Modes of Implementation

- The implementation has support for two modes of operation, `sequential` and `distributed`.
- In the `sequnetial` mode, the map and reduce tasks are executed one at a time: first, the first map task is executed to completion, then the second, then the third, etc. When all the map tasks have finished, the first reduce task is run, then the second, etc.
- This mode, while not very fast, is useful for debugging.
- The `distributed` mode runs many worker threads that first execute map tasks in parallel, and then reduce tasks. This is much faster, but also harder to implement and debug.
