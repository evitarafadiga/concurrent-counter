# Custom String Counter

## Problem Description

You are part of a team that develops a real-time data platform for bid enrichment and integrates with high throughput and low latency clients (typical integrations need to handle 1 million requests per second in less than 7ms response time including the network trip). The HTTP server returns unique codes as string values. However, the business needs to know how many of these unique codes have been returned at any given time. Your task is to develop a thread-safe, zero-allocation, ultra-fast and optimized package that counts those unique codes and reports them periodically into an output. This is not a normal HTTP server; every CPU cycle and every allocation matters.

You have to develop a simple package that receives content and counts the unique codes. The unique codes are one blob of bytes separated by the newline character `'\n'`:  
> e.g. AU_ieu13\n103956\nghgqb\n10002\na012ne
>
> The expected outcome of the function is: 
>  1. AU_ieu13
>  2. 103956
>  3. ghgqb
>  4. 10002
>  5. a012ne

You don't have to worry about duplicate values. 

The function will be added to the _hotpath_. The _hotpath_ is your main handler where all the heavy processing takes place. Adding a function call to the main handler must not add overhead, otherwise we will miss the 7ms threshold. 

The code must handle concurrent access from at least 100 goroutines simultaneously. Implement periodic reporting (every 1 second) of concurrent counts to multiple concurrent subscribers with different reporting intervals (1s, 5s, 30s).

You don't have to develop the output; assume that each subscriber implements the io.WriteCloser interface and is registered via a Subscribe() method.

Proper use of channels to communicate between hotpath (producer) and the reporting components is critical.  

```go
package response_analyser

import (
    "context"
    "io"
    "time"
)

type DataExporter interface {
    // This is your entrypoint from the hotpath. The hotpath will call this function and it will pass the response content before it writes it to the http.Response body.
    Analyse(content []byte)
    // This function should return the current values of each unique code. It should not reset the counters though.
    GetCurrentCounts() map[string]uint64
    // Subscribe adds a new reporting subscriber with specified interval
    Subscribe(writer io.WriteCloser, interval time.Duration) error
    // Shutdown is expected to be called by the main app when the server receives a termination call with context for graceful shutdown.
    Shutdown(ctx context.Context) error
}

// New should not be just an empty constructor. Feel free to change the signature of this function in order to accept the necessary parameters of your solution.
func New() DataExporter { /* TODO */ }

// your code
```

_Note: the above code is a guide only. Feel free to add more methods to the interface._ 

Prepare yourself to explain why you took certain decisions on your code.

## Architecture Guide
- Implement a producer-consumer pattern using buffered channels
- Support graceful shutdown with context cancellation and proper resource cleanup
- Handle backpressure when consumers are slow
- Implement custom memory pools for different object types to minimize GC pressure
- Proper use of sync.Pool, Mutex and atomic operations.

## What We Test

1. **Concurrency** - Safe access from multiple goroutines simultaneously  
2. **Zero Allocations** - No memory allocations wherever possible
3. **Error Handling** - Handle corrupted input data and network timeout scenarios gracefully
4. **Performance** - Sub-millisecond processing time under normal load

_Bonus_  

- **Standard Library** - Prefer the standard library; use of sync and atomic packages is a plus.

We expect to see proper tests and benchmarks that verify your work. Code coverage is a plus.

## Submission Instructions

1. **Create a new public GitHub repository**
   - Initialize with the prompt as `CHALLENGE.md`

2. **Implement on a feature branch**
   - Create branch: `git checkout -b solution`
   - Implement your solution with tests and benchmarks
   - Document your approach and decisions in README.md

3. **Create a Pull Request**
   - Open PR from `solution` branch to `master`
   - Include in PR description:
     - Your approach and design decisions
     - Performance optimizations
     - How to run tests and benchmarks
   - **Do not merge the PR**

4. **Grant access**
   - Add collaborators if it's a private repository
   - Email us the repository URL and PR link

## Git tags to include 

1. pavlosaudigent
2. rickgiles-ad
