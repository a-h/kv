```
curl -s -H "Authorization: bearer $GITHUB_TOKEN" \
  -X POST https://api.github.com/graphql \
  -d '{
    "query": "query { repository(owner:\"a-h\", name:\"kv\") { pullRequest(number:1) { reviewThreads(first:100) { nodes { isResolved comments(first:100) { nodes { author { login } body createdAt path position }}}}}}}"
  }' | jq
```

 STILL NEED TO CHECK (May have been addressed):
Streaming & Performance:
graph.go Position 116: Streaming operations without collecting to slices
graph.go Position 162: Same streaming issue with GetAllIncoming/GetIncoming
graph.go Position 313: Non-deterministic output from map iteration
API Improvements:
graph.go Position 103: Support for "all" in addition to "*" for edge-type
graph.go Position 290: Functions taking io.Writer instead of fmt.Println
graph.go Position 261: Whether GetAllOutgoing is still duplicate of GetOutgoing
Constructor & Architecture:
graph.go Positions 59, 73: Too many constructors issue

❌ LIKELY STILL OUTSTANDING (Not related to graph changes):

Storage Backend Issues:
store.go Position 21: Use slices.Chunk from newer Go
store.go Positions 4, 21: Batch size questions and slices.Chunk
store.go Position 21: Use slices.Chunk
Would you like me to check specific files to see exactly which of the remaining comments have been addressed in your commits