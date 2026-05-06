# These are a set of rules that SHOULD be enforced throughout codebase to avoid bugs

1. After (or before) inserting into a map (any type), we should check if the key is being replaced or the insertion was successful.
2. Before writing to any buffers, you should check its size. If not enough, FATAL.