<p align="center">
    <img src="https://github.com/madlitz/warp/assets/4271492/8d500e6a-e67b-48e9-9feb-06390fb8277c">
</p>

# Warp
Warp is a Go package that provides a simple way to run a set of functions in the correct order and gather the output.
It combines paradigms of dependency injection and pipeline execution to provide a flexible and powerful way to run functions.

It uses reflection during initialization to build a dependency graph of the functions and then runs them in the correct order when the engine is executed. Dependencies are resolved using the input and output types of the functions
parameters and return values.

<img width="742" alt="Warp Function Diagram" src="https://github.com/madlitz/warp/assets/4271492/05da1e3f-4fe1-4cfa-9c25-a6c617ac7825">

In the diagram above the you initiate the engine with the 5 funcs where `a`-`j` are params with unique types. 
You then run the engine as many times as you like with your available inputs which are any types not produced as outputs by your funcs, i.e. in this case `a`, `b`, `d` and `i`.

```myVar, err := warp.Run[j](ctx, ngn, a, b, d, i)``` will produce a return value `myVar` with type `j`.

### Functions
There are a number of conditions that you must adhere to when defining your functions.
* each function MUST:
    - be of type function.
    - return at least one non error output.
    - return at most one error output.
    - NOT accept an `error` type parameter.
    - NOT return a `context.Context` type output.
    - NOT output any types that overlap with the function parameter types
    - NOT accept variadic parameters
    - NOT repeat paramater types

* all functions MUST:
    - NOT have overlapping output types.
    - NOT contain cyclic dependencies between function inputs and outputs

### Errors
You can add an `error` return value to any of your functions. If one function returns an error, all functions will immediately return and the `Run` call will return that error.

### Context
If your function has blocking I/O you can add `context.Context` to your input and it will be cancelled if an error occurs.

### Concurrency
All functions will run concurrently in their own Goroutine as soon as their inputs are ready.

### Optional parameters
By default if a function (or one of its upstream functions) does not have the input it requires from the parameters passed to the `Run` function, it will not run.
If however, the input that was missing was declared wrapped in `warp.Optional[A]` it will run regardless, where `warp.Optional[A].Set` will be true if the upstream function ran, false otherwise. 
This is called an optional input.

You may also declare an output of a function as optional by wrapping it in `warp.Optional`. In this case, the output of the function is considered to be missing by downstream 
functions if `warp.Optional[A].Set == false`. So `func(A) B` would NOT run in this case.

If both an output of one function, `func(A) warp.Optional[B]` and the input to another, `func(warp.Optional[B]) C` are both optional, then the downstream function will run as
expected passing through both `B.Value` and `B.Set`.


## Installation

```bash
go get github.com/madlitz/warp
```

## Usage

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
    "time"

    "github.com/madlitz/warp"
)

// Define type aliases for each unique type for this example
type A int
type B string
type C struct {
    Value float64
}
type D int
type E []string
type F struct {
    Double int
}
type G *bool
type H int
type I int
type J struct {
    I int  `json:"i"`
    G bool `json:"g"`
    F int  `json:"f"`
    H int  `json:"h"`
}

func main() {
    // Initialize the engine
    engine, err := warp.Initialize(
        func(ctx context.Context, a A, b B) (c C, err error) {
            // Simulate a database call
            dbResult, err := func(ctx context.Context) (string, error) {
                select {
                case <-time.After(2 * time.Second): // Simulate a delay
                    return "dbResult", nil
                case <-ctx.Done():
                    return "", ctx.Err()
                }
            }(ctx)
            if err != nil {
                return C{}, err
            }
            log.Println("DB Result:", dbResult)
            return C{Value: float64(len(b) * int(a))}, nil
        },
        func(ctx context.Context, c C) (g G, err error) {
            result := c.Value > 10
            return &result, nil
        },
        func(d D) (e E, f warp.Optional[F]) {
            if d == 2 {
                // Set optional output to not set if d is 2
                return E{"hello"}, warp.Optional[F]{Set: false}
            }
            return E{"hello"}, warp.Optional[F]{Value: F{Double: int(d * 2)}, Set: true}
        },
        func(e E, f F) (h H) {
            // I don't run because F.Set is false, therefore H will be == 0
            return H(len(e))
        },
        func(i I, g G, f F, h warp.Optional[H]) (j J) {
            return J{
                I: int(i),
                G: *g,
                F: f.Double,
                H: int(h.Value),
            }
        },
    )
    if err != nil {
        panic(err)
    }

    // HTTP handler function to run the engine
    http.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
        // Here you can extract inputs from the request. For simplicity, we'll use hardcoded values.
        a, b, d, i := A(3), B("test"), D(2), I(5)

        // Run the engine with initial inputs a, b, d, and i
        j, err := warp.Run[J](context.Background(), engine, a, b, d, i)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        // Encode the result as JSON and write it to the response
        w.Header().Set("Content-Type", "application/json")
        if err := json.NewEncoder(w).Encode(j); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
        }

        /*
        res = {
            "i": 5,
            "g": false,
            "f": 4,
            "h": 0
        }
        */
    })

    // Start the HTTP server
    log.Println("Server started at :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}


```
## Real-world use case
The use case that inspired this package was the concept of 'Analyzers' running against an API request containing a number of pieces of 'Evidence'.
Each analyzer may analyze different pieces of evidence and also take into account the result of other analyzers.
Some analyzers call out to 3rd party APIs and Databases while others simply perform a calculation or if-then-else descision.

The last analyzer aggregates the result of all other analyzers so we have a record of how each decision is made.

## Benefits
The end result is that each function you define is decoupled from how and when it is executed. 
This makes composing functions together much easier as you don't have to write all the boilerplate 
to pass outputs to inputs and decide on the order of execution.

It can also assist in unit testing as each function can be tested in isolation, knowing that the engine
will always execute the functions together in the same deterministic way.


## License

MIT

## Author

madlitz

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request if you find a bug or want to add a new feature.

## Acknowledgements

This package was inspired by the [topological sort](https://en.wikipedia.org/wiki/Topological_sorting) algorithm.


## Changelog

- 0.1.0
  - Initial release
