# Distributed Systems

This repository contains my solutions to the infamous [`MIT 6.824 2017 Course`](http://nil.csail.mit.edu/6.824/2017/index.html) on `Distributed Systems`.

- The primary aim of this project is to learn about distributed systems by implementing all the proposed algorithms. Though they might not be used directly in applications today, they form a good base to get started.
- This repository also migrates all the labs from `Go 1.7` to the lastest `Go 1.21` and in the process restructures the code and project to improve readibility and accessibility.
- I also try to add more methods as and when needed to make the code more "pure" and use idiomatic Go as much as I can.
- Each folder has it's own `README.md` which should provide the necessary context about the technique implemented in the same.
- Knowledge of `Go` and it's standard packages like `net`, `rpc`, `io`, `os` etc. is essential for the purpose of the same.

## Progress

<table>
  <tr>
    <th>Lab</th>
    <th>Task ID</th>
    <th>Task Description</th>
    <th>Status</th>
  </tr>

  <tr>
    <th rowspan="5">
        <a href="./mapreduce/README.md">
            Lab 1 
            <br>
            <br>
            Map Reduce Algorithm 
        </a>
    </th>
    <td>1</td>
    <td>MapReduce Input and Output</td>
    <td>✅</td>
  </tr>
  <tr>
    <td>2</td>
    <td>Single Worker Work Count</td>
    <td>✅</td>
  </tr>
    <tr>
    <td>3</td>
    <td>Distributing MapReduce tasks</td>
    <td>⏳</td>
  </tr>
  </tr>
    <td>4</td>
    <td>Handling worker failures</td>
    <td>🔴</td>
  </tr>
  </tr>
    <td>5</td>
    <td>Inverted index generation (Optional)</td>
    <td>🔴</td>
  </tr>
</table>
