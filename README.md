# Warp

Warp is a Go package that provides a simple way to run a set of functions in the correct order and gather the output.
It combines paradigms of dependency injection and pipeline execution to provide a flexible and powerful way to run functions.

It uses reflection during initialization to build a dependency graph of the functions and then runs them in the correct order when the engine is executed. Dependencies are resolved using the input and output types of the functions
parameters and return values.

## Installation

```bash
go get github.com/dezlitz/warp
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/dezlitz/warp"
)

func main() {
    // Initialize the engine with a set of functions
    engine, err := warp.Initialize(
        func() int { return 1 },
        func(a int) double { return a * 2.5 },
        func(b double) out1[string] { return fmt.Sprintf("Result: %d", b) },
        func(c string, d int, e out1[string]) out2[string] { return fmt.Sprintf("Result (Extended): Name: %s, Input: %s, Result: %s", c, d, e) },
    )
    if err != nil {
        log.Fatal(err)
    }

    // Run the engine
    results, err := warp.Run[out2[string]](context.Background(), engine, "Engine Test")
    if err != nil {
        log.Fatal(err)
    }

    // Print the results
    for _, result := range results {
        fmt.Println(result)
    }

    // Result: 2.5
    // Result (Extended): Name: Engine Test, Input: 1, Result: 2.5
}
```

## License

MIT

## Author

dezlitz

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request if you find a bug or want to add a new feature.

## Acknowledgements

This package was inspired by the [topological sort](https://en.wikipedia.org/wiki/Topological_sorting) algorithm.

## References

- [Go Reflection](https://blog.golang.org/laws-of-reflection)
- [Go Sync Map](https://pkg.go.dev/sync#Map)
- [Go Errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup)
- [Go Context](https://pkg.go.dev/context)
- [Go Type Assertions](https://tour.golang.org/methods/15)
- [Go Type Conversions](https://tour.golang.org/methods/9)
- [Go Type Switches](https://tour.golang.org/methods/16)

## Changelog

- 0.1.0
  - Initial release
