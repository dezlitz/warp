package warp_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	. "github.com/dezlitz/warp"
)

type (
	outType1 string
	outType2 string
	outType3 string
	outType4 string
	outType5 string
	outType6 string
	inType   string
	optType3 = Optional[outType3]
)

func Test_EngineInit(t *testing.T) {
	t.Run("should initialise the engine successfully", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(inType) outType1 { return "" },
			func(context.Context, inType, outType1) (outType2, error) { return "", nil },
			func(context.Context, inType, outType2, outType1) (outType3, outType4, error) { return "", "", nil },
			func(context.Context, inType, outType2, Optional[outType1]) (outType5, error) { return "", nil },
			func(context.Context, inType, outType2, Optional[outType1]) (outType6, error) { return "", nil },
		)
		if err != nil {
			t.Fatal(err)
		}

		if ngn == nil {
			t.Fatalf("failed to construct a %T", ngn)
		}
	})

	t.Run("should return an error if engine is initialized with no functions", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize()

		assertErr(t, err, "input validation error: engine must be initialized with at least one function")
	})
	t.Run("should return an error if any of the passed arguments is not a function", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(context.Context, inType) (outType1, error) {
				return "", nil
			},
			"<not-a-function>",
			func(context.Context, inType) (outType2, outType3, error) {
				return "", "", nil
			},
		)

		assertErr(t, err, "input string caused validation error: all inputs must be functions")
	})

	t.Run("should return an error if two or more functions return the same type", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) outType1 {
				return ""
			},
			func(context.Context, inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, outType3, error) {
				return "", "", nil
			},
		)

		assertErrContains(t, err, "output value type warp_test.outType1 already provided")
	})

	t.Run("should return an error if any of the functions has no return values", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) {
				return
			},
			func(context.Context, inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, outType3, error) {
				return "", "", nil
			},
		)

		assertErrContains(t, err, "must not have no return type(s)")
	})

	t.Run("should return an error if any of the functions does not return any non-error values", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, error) {
				return "", nil
			},
			func(context.Context, inType) error {
				return nil
			},
		)

		assertErrContains(t, err, "must have at least 1 return value type (excluding error)")
	})
	t.Run("should return an error if any of the functions only returns multiple errors", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, error) {
				return "", nil
			},
			func(context.Context, inType) (outType3, error, error) {
				return "", nil, nil
			},
		)

		assertErrContains(t, err, "must have no more than 1 error return type")
	})

	t.Run("should return an error if any of the functions takes and returns the same type values", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) (inType, error) {
				return "", nil
			},
			func(context.Context, inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, outType3, error) {
				return "", "", nil
			},
		)

		assertErrContains(t, err, "input type warp_test.inType is also an output type")

		t.Run("when wrapped in optional type", func(t *testing.T) {
			t.Parallel()
			_, err := Initialize(
				func(Optional[inType]) (inType, error) {
					return "", nil
				},
				func(context.Context, inType) (outType1, error) {
					return "", nil
				},
				func(context.Context, inType) (outType2, outType3, error) {
					return "", "", nil
				},
			)

			assertErrContains(t, err, "input type warp_test.inType is also an output type")
		})
	})

	t.Run("should return an error if any of the functions return any of optional types", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, error) {
				return "", nil
			},
			func(context.Context, inType) (optType3, error) {
				return optType3{}, nil
			},
		)

		assertErrContains(t, err, "outType3] must not be an optional type")
	})

	t.Run("should return an error if at least two functions are cyclically dependent on each other", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(context.Context, inType) (outType1, error) {
				return "", nil
			},
			func(outType1) (outType2, error) {
				return "", nil
			},
			func(outType2) (inType, error) {
				return "", nil
			},
		)

		assertErrContains(t, err, "cyclic dependency detected")

		t.Run("when wrapped in optional type", func(t *testing.T) {
			_, err := Initialize(
				func(context.Context, inType) (outType1, error) {
					return "", nil
				},
				func(Optional[outType1]) (outType2, error) {
					return "", nil
				},
				func(Optional[outType2]) (inType, error) {
					return "", nil
				},
			)

			assertErrContains(t, err, "cyclic dependency detected")
		})
	})

	t.Run("should return an error if any function takes in an error", func(t *testing.T) {
		t.Parallel()
		fn1 := func(error) (outType2, error) {
			return "", nil
		}
		_, err := Initialize(
			fn1,
			func(outType1) (outType2, error) {
				return "", nil
			},
			func(outType2) (outType3, error) {
				return "", nil
			},
		)

		assertErrContains(t, err, "must not have input param(s) of type error")
	})

	t.Run("should return an error if any function returns a context", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType) (outType1, error) {
				return "", nil
			},
			func(outType1) (outType2, error) {
				return "", nil
			},
			func(outType2) (context.Context, error) {
				return context.Background(), nil
			},
		)

		assertErrContains(t, err, "must not have any context.Context return value type(s)")
	})

	t.Run("should return an error if any function takes the same parameter type more than once", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(inType, inType) outType1 { return "" },
			func(outType1) (outType2, error) {
				return "", nil
			},
			func(outType2) (outType3, error) {
				return "", nil
			},
		)

		assertErrContains(t, err, "function takes the same parameter type warp_test.inType more than once")

		t.Run("when wrapped in optional type", func(t *testing.T) {
			_, err := Initialize(
				func(inType, Optional[inType]) outType1 { return "" },
				func(outType1) (outType2, error) {
					return "", nil
				},
				func(outType2) (outType3, error) {
					return "", nil
				},
			)

			assertErrContains(t, err, "function takes the same parameter type warp_test.inType more than once")
		})
	})

	t.Run("should return an error if any function takes variadic parameters", func(t *testing.T) {
		t.Parallel()
		_, err := Initialize(
			func(context.Context, ...inType) (outType1, error) {
				return "", nil
			},
			func(context.Context, inType) (outType2, outType3, error) {
				return "", "", nil
			},
		)

		assertErrContains(t, err, "must not be a variadic function")
	})
}

type (
	interfaceType interface{ String() string }
	concreteType  struct{ ValueOut string }
)

func (c concreteType) String() string { return c.ValueOut }

func Test_EngineRun(t *testing.T) {
	type (
		outType1       struct{ ValueOut1 string }
		outType2       struct{ ValueOut2 string }
		outType3       struct{ ValueOut3 string }
		outType4       struct{ ValueOut4 string }
		outType5       struct{ ValueOut5 string }
		outType6       struct{ ValueOut6 string }
		genType[T any] outType5
		inType1        struct{ ValueIn1 string }
		inType2        struct{ ValueIn2 string }
		inType3        struct{ ValueIn3 string }
		inType4        struct{ ValueIn4 string }
		inType5        struct{ ValueIn5 string }
	)

	t.Run("should run the engine successfully", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(ctx context.Context, in outType1) (*outType2, error) {
				return &outType2{in.ValueOut1 + "<outType2>"}, nil
			},
			func(ctx context.Context, in *outType2) outType3 {
				return outType3{in.ValueOut2 + "<outType3>"}
			},
			func(ctx context.Context, in concreteType, in2 outType3) (interfaceType, error) {
				return concreteType{in.ValueOut + in2.ValueOut3 + "<outTypeInterface>"}, nil
			},
			func(ctx context.Context, in interfaceType) (outType4, error) {
				return outType4{in.String() + "<outType4>"}, nil
			},
			func(in inType1) outType1 { return outType1{in.ValueIn1 + "<outType1>"} },
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType4](
			ctx,
			ngn,
			inType1{"<inType>"},
			concreteType{"<inTypeConcrete>"},
		)
		assert.NoError(t, err)
		assert.Len(t, out, 1)
		assert.Equal(t, "<inTypeConcrete><inType><outType1><outType2><outType3><outTypeInterface><outType4>", out[0].ValueOut4)
	})

	t.Run("should collect all outputs if Run type is any", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(ctx context.Context, in outType1) (outType2, error) {
				return outType2{in.ValueOut1 + "<outType2>"}, nil
			},
			func(ctx context.Context, in outType2) (outType3, error) {
				return outType3{in.ValueOut2 + "<outType3>"}, nil
			},
			func(ctx context.Context, in outType3) (outType4, error) {
				return outType4{in.ValueOut3 + "<outType4>"}, nil
			},
			func(in inType1) outType1 { return outType1{in.ValueIn1 + "<outType1>"} },
		)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[any](
			ctx,
			ngn,
			inType1{"<inType>"},
		)
		assert.NoError(t, err)

		expected := []any{
			outType1{"<inType><outType1>"},
			outType2{"<inType><outType1><outType2>"},
			outType3{"<inType><outType1><outType2><outType3>"},
			outType4{"<inType><outType1><outType2><outType3><outType4>"},
		}
		assert.IsType(t, expected, out)
		assert.Len(t, out, len(expected))
		assert.ElementsMatch(t, out, expected)
	})

	t.Run("should run the engine successfully collecting a generic type annotation returned by collecting its concrete type", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(ctx context.Context, in outType1) (outType2, error) {
				return outType2{in.ValueOut1 + "<outType2>"}, nil
			},
			func(ctx context.Context, in outType2) (genType[int], error) {
				return genType[int]{in.ValueOut2 + "<outType3>"}, nil
			},
			func(ctx context.Context, in genType[int]) (genType[float64], error) {
				return genType[float64]{in.ValueOut5 + "<outType4>"}, nil
			},
			func(in inType1) outType1 { return outType1{in.ValueIn1 + "<outType1>"} },
		)
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType5](
			ctx,
			ngn,
			inType1{"<inType>"},
		)
		assert.NoError(t, err)

		expected := []outType5{
			{ValueOut5: "<inType><outType1><outType2><outType3>"},
			{ValueOut5: "<inType><outType1><outType2><outType3><outType4>"},
		}
		assert.IsType(t, expected, out)
		assert.Len(t, out, len(expected))
		assert.ElementsMatch(t, out, expected)
	})

	t.Run("should execute 2 sets of functions with unrelated dependencies", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int32
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			func(ctx context.Context, in outType1) (outType2, error) {
				count.Add(1)
				return outType2{in.ValueOut1 + "<outType2>"}, nil
			},
			func(ctx context.Context, in inType2) (outType4, error) {
				count.Add(1)
				return outType4{in.ValueIn2 + "<outType4>"}, nil
			},
			func(ctx context.Context, in outType4) (outType5, error) {
				count.Add(1)
				return outType5{in.ValueOut4 + "<outType5>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType5](
			ctx,
			ngn,
			inType1{"<inType1>"},
			inType2{"<inType2>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 4 {
			t.Fatalf("expected 4 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}

		if expected := "<inType2><outType4><outType5>"; out[0].ValueOut5 != expected {
			t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
		}
	})

	t.Run("should execute sequential functions the same regardless of the input order", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int32
		// run 1
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			func(ctx context.Context, in inType2) (outType2, error) {
				count.Add(1)
				return outType2{in.ValueIn2 + "<outType2>"}, nil
			},
			func(ctx context.Context, in outType2) (outType3, error) {
				count.Add(1)
				return outType3{in.ValueOut2 + "<outType3>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType3](
			ctx,
			ngn,
			inType1{"<inType1>"},
			inType2{"<inType2>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 3 {
			t.Fatalf("expected 3 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}

		if expected := "<inType2><outType2><outType3>"; !strings.Contains(out[0].ValueOut3, expected) {
			t.Fatalf("expected output value contains '%s', got '%s'", expected, out[0])
		}

		count.Swap(0)
		// run 2 in different order
		ngn, err = Initialize(
			func(ctx context.Context, in outType2) (outType3, error) {
				count.Add(1)
				return outType3{in.ValueOut2 + "<outType3>"}, nil
			},
			func(ctx context.Context, in inType2) (outType2, error) {
				count.Add(1)
				return outType2{in.ValueIn2 + "<outType2>"}, nil
			},
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		out, err = Run[outType3](
			ctx,
			ngn,
			inType1{"<inType1>"},
			inType2{"<inType2>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 3 {
			t.Fatalf("expected 3 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}

		if expected := "<inType2>"; !strings.Contains(string(out[0].ValueOut3), expected) {
			t.Fatalf("expected output value contains '%s', got '%s'", expected, out[0])
		}
		if expected := "<outType2>"; !strings.Contains(string(out[0].ValueOut3), expected) {
			t.Fatalf("expected output value contains '%s', got '%s'", expected, out[0])
		}
		if expected := "<outType3>"; !strings.Contains(string(out[0].ValueOut3), expected) {
			t.Fatalf("expected output value contains '%s', got '%s'", expected, out[0])
		}
	})

	t.Run("should collect all outputs that are convertible to the Run type", func(t *testing.T) {
		t.Parallel()
		type (
			outType1 string
			outType2 string
			outType3 string
			outType4 string
			inType1  string
		)
		ngn, err := Initialize(
			func(ctx context.Context, in outType1) (outType2, error) {
				return outType2(in + "<outType2>"), nil
			},
			func(ctx context.Context, in outType2) (outType3, error) {
				return outType3(in + "<outType3>"), nil
			},
			func(ctx context.Context, in outType3) (outType4, error) {
				return outType4(in + "<outType4>"), nil
			},
			func(in inType1) outType1 { return outType1(in + "<outType1>") },
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := Run[string](
			ctx,
			ngn,
			inType1("<inType>"),
		)
		if err != nil {
			t.Fatal(err)
		}

		if len(out) != 4 {
			t.Fatalf("expected 4 output values, got %d", len(out))
		}

		expected := []string{
			"<inType><outType1>",
			"<inType><outType1><outType2>",
			"<inType><outType1><outType2><outType3>",
			"<inType><outType1><outType2><outType3><outType4>",
		}

		if cmp.Diff(expected, out, stringSliceTransformer) != "" {
			t.Fatalf("expected output value '%s', got '%s'", expected, out)
		}
	})

	t.Run("should execute 2 sets of functions with divergent dependencies", func(t *testing.T) {
		t.Parallel()

		var count atomic.Int32
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{ValueOut1: in.ValueIn1 + "<outType1>"}
			},
			func(_ context.Context, in inType1, _ inType2) (outType2, outType5, error) {
				count.Add(1)
				return outType2{in.ValueIn1 + "<outType2>"}, outType5{in.ValueIn1 + "<outType5>"}, nil
			},
			func(_ context.Context, in outType2) (outType3, error) {
				count.Add(1)
				return outType3{in.ValueOut2 + "<outType3>"}, nil
			},
			func(_ context.Context, in outType2) (outType4, error) {
				count.Add(1)
				return outType4{in.ValueOut2 + "<outType4>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("first path <outType4>", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := Run[outType4](
				ctx,
				ngn,
				inType1{"<inType1>"},
				inType2{"<inType2>"},
			)
			if err != nil {
				t.Fatal(err)
			}

			if count.Load() != 4 {
				t.Fatalf("expected 4 function calls, got %d", count.Load())
			}

			if len(out) != 1 {
				t.Fatalf("expected 1 output value, got %d", len(out))
			}

			if expected := "<inType1><outType2><outType4>"; out[0].ValueOut4 != expected {
				t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
			}
		})

		count.Swap(0)

		t.Run("second path <outType3>", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := Run[outType3](
				ctx,
				ngn,
				inType1{"<inType1>"},
				inType2{"<inType2>"},
			)
			if err != nil {
				t.Fatal(err)
			}

			if count.Load() != 4 {
				t.Fatalf("expected 4 function calls, got %d", count.Load())
			}

			if len(out) != 1 {
				t.Fatalf("expected 1 output value, got %d", len(out))
			}

			if expected := "<inType1><outType2><outType3>"; out[0].ValueOut3 != expected {
				t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
			}
		})
	})

	t.Run("should execute 2 sets of functions with convergent dependencies", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int32
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			func(_ context.Context, in inType1, _ inType2) (outType2, outType5, error) {
				count.Add(1)
				return outType2{in.ValueIn1 + "<outType2>"}, outType5{in.ValueIn1 + "<outType5>"}, nil
			},
			func(_ context.Context, in outType1, in2 outType2) (outType3, error) {
				count.Add(1)
				return outType3{in.ValueOut1 + in2.ValueOut2 + "<outType3>"}, nil
			},
			func(_ context.Context, in outType3) (outType4, error) {
				count.Add(1)
				return outType4{in.ValueOut3 + "<outType4>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := Run[outType4](
			ctx,
			ngn,
			inType1{"<inType1>"},
			inType2{"<inType2>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 4 {
			t.Fatalf("expected 4 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}
		if expected := "<inType1><outType1><inType1><outType2><outType3><outType4>"; out[0].ValueOut4 != expected {
			t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
		}
	})

	t.Run("should ignore execution of the functions that don't have input values provided", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int32
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			func(_ context.Context, in inType2) (outType2, outType3, error) {
				count.Add(1)
				return outType2{in.ValueIn2 + "<outType2>"}, outType3{in.ValueIn2 + "<outType3>"}, nil
			},
			func(_ context.Context, in outType1) (outType4, error) {
				count.Add(1)
				return outType4{in.ValueOut1 + "<outType4>"}, nil
			},
			func(_ context.Context, in outType4) (outType5, error) {
				count.Add(1)
				return outType5{in.ValueOut4 + "<outType5>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType5](
			ctx,
			ngn,
			inType1{"<inType1>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 3 {
			t.Fatalf("expected 3 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}
		if expected := "<inType1><outType1><outType4><outType5>"; out[0].ValueOut5 != expected {
			t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
		}
	})

	t.Run("optional parameters", func(t *testing.T) {
		t.Parallel()

		t.Run("when corresponding arguments provided", func(t *testing.T) {
			t.Run("from initial input values", func(t *testing.T) {
				var count atomic.Int32
				ngn, err := Initialize(
					func(_ context.Context, in Optional[inType1]) (outType1, error) {
						count.Add(1)
						v, _ := in.Value()
						return outType1{v.ValueIn1 + "<outType1>"}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[any](
					ctx,
					ngn,
					inType1{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				if count.Load() != 1 {
					t.Fatalf("expected 1 function call, got %d", count.Load())
				}

				assert.Contains(t, out, any(outType1{ValueOut1: "<inType1><outType1>"}))
			})

			t.Run("from the result of the execution of an upstream function", func(t *testing.T) {
				var count atomic.Int32
				ngn, err := Initialize(
					func(_ context.Context, in inType1) (outType1, error) {
						count.Add(1)
						return outType1{in.ValueIn1 + "<outType1>"}, nil
					},
					func(_ context.Context, in Optional[outType1]) (outType2, error) {
						count.Add(1)
						v, _ := in.Value()
						return outType2{v.ValueOut1 + "<outType2>"}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[any](
					ctx,
					ngn,
					inType1{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				if count.Load() != 2 {
					t.Fatalf("expected 2 function calls, got %d", count.Load())
				}

				assert.Contains(t, out, any(outType2{ValueOut2: "<inType1><outType1><outType2>"}))
			})

			t.Run("value is set", func(t *testing.T) {
				var count atomic.Int32
				var wasSet atomic.Bool
				ngn, err := Initialize(
					func(_ context.Context, in Optional[inType2]) (outType1, error) {
						count.Add(1)
						v, ok := in.Value()
						wasSet.Store(ok)
						return outType1{ValueOut1: v.ValueIn2}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[outType1](
					ctx,
					ngn,
					inType2{"<inType2>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				assert.True(t, wasSet.Load(), "isSet is false")

				if count.Load() != 1 {
					t.Fatalf("expected 1 function call, got %d", count.Load())
				}

				assert.Contains(t, out, outType1{ValueOut1: "<inType2>"})
			})
		})

		t.Run("when corresponding arguments NOT provided", func(t *testing.T) {
			t.Run("as initial input values", func(t *testing.T) {
				var count atomic.Int32
				ngn, err := Initialize(
					func(_ context.Context, in Optional[inType2]) (outType1, error) {
						count.Add(1)
						v, _ := in.Value()
						return outType1{v.ValueIn2 + "<outType1>"}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[any](
					ctx,
					ngn,
					inType1{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				if count.Load() != 1 {
					t.Fatalf("expected 1 function call, got %d", count.Load())
				}

				assert.Contains(t, out, any(outType1{ValueOut1: "<outType1>"}))
			})
			t.Run("as the result of the execution of an upstream function", func(t *testing.T) {
				var count atomic.Int32
				ngn, err := Initialize(
					func(_ context.Context, in inType2) (outType1, error) {
						count.Add(1)
						return outType1{in.ValueIn2 + "<outType1>"}, nil
					},
					func(_ context.Context, in Optional[outType1]) (outType2, error) {
						count.Add(1)
						v, _ := in.Value()
						return outType2{v.ValueOut1 + "<outType2>"}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[any](
					ctx,
					ngn,
					inType1{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				if count.Load() != 1 {
					t.Fatalf("expected 1 function call, got %d", count.Load())
				}

				assert.Contains(t, out, any(outType2{ValueOut2: "<outType2>"}))
			})

			t.Run("no value is set", func(t *testing.T) {
				var count atomic.Int32
				var wasSet atomic.Bool
				ngn, err := Initialize(
					func(_ context.Context, in Optional[inType2]) (outType1, error) {
						count.Add(1)
						v, ok := in.Value()
						wasSet.Store(ok)
						return outType1{ValueOut1: v.ValueIn2}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[outType1](
					ctx,
					ngn,
					inType1{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				assert.False(t, wasSet.Load(), "isSet is true")

				if count.Load() != 1 {
					t.Fatalf("expected 1 function call, got %d", count.Load())
				}

				assert.Contains(t, out, outType1{ValueOut1: ""})
			})
		})

		t.Run("when used along with required parameters", func(t *testing.T) {
			t.Run("should execute if value for required parameter is supplied", func(t *testing.T) {
				var count atomic.Int32
				ngn, err := Initialize(
					func(_ context.Context, in inType1, in2 Optional[inType2]) (outType1, error) {
						count.Add(1)
						v, _ := in2.Value()
						return outType1{in.ValueIn1 + v.ValueIn2 + "<outType1>"}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				out, err := Run[any](
					ctx,
					ngn,
					inType1{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				if count.Load() != 1 {
					t.Fatalf("expected 1 function call, got %d", count.Load())
				}

				assert.Contains(t, out, any(outType1{ValueOut1: "<inType1><outType1>"}))
			})
			t.Run("should not execute if value for required parameter is not supplied", func(t *testing.T) {
				var count atomic.Int32
				ngn, err := Initialize(
					func(_ context.Context, in inType1, in2 Optional[inType2]) (outType1, error) {
						count.Add(1)
						v, _ := in2.Value()
						return outType1{in.ValueIn1 + v.ValueIn2 + "<outType1>"}, nil
					},
				)
				if err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, err = Run[any](
					ctx,
					ngn,
					inType2{"<inType1>"},
				)
				if err != nil {
					t.Fatal(err)
				}

				if count.Load() != 0 {
					t.Fatalf("expected 0 function calls, got %d", count.Load())
				}
			})
		})
	})

	t.Run("should propagate the context to the functions", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ctx = context.WithValue(ctx, "key", "value")

		ngn, err := Initialize(
			func(in inType1) outType1 { return outType1{in.ValueIn1 + "<outType1>"} },
			func(ctx context.Context, in outType1) (outType2, error) {
				if ctx.Value("key") != "value" {
					return outType2{}, errors.New("context not propagated")
				}
				return outType2{in.ValueOut1 + "<outType2>"}, nil
			},
			func(ctx context.Context, in outType2) (outType3, error) {
				if ctx.Value("key") != "value" {
					return outType3{}, errors.New("context not propagated")
				}
				return outType3{in.ValueOut2 + "<outType3>"}, nil
			},
			func(ctx context.Context, in outType3) (outType4, error) {
				if ctx.Value("key") != "value" {
					return outType4{}, errors.New("context not propagated")
				}
				return outType4{in.ValueOut3 + "<outType4>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		if _, err = Run[outType4](
			ctx,
			ngn,
			inType1{"<inType>"},
		); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("should return an error if the engine is not initialised", func(t *testing.T) {
		t.Parallel()
		if _, err := Run[outType4](
			context.Background(),
			&Engine{},
			inType1{"<inType>"},
		); err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("should immediately return an error if one of the functions returns an error", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(in inType1) outType1 { return outType1{in.ValueIn1 + "<outType1>"} },
			func(ctx context.Context, in outType1) (outType2, error) {
				return outType2{in.ValueOut1 + "<outType2>"}, nil
			},
			func(ctx context.Context, in outType2) (*outType3, error) {
				return nil, errors.New("<error>")
			},
			func(ctx context.Context, in *outType3) (outType4, error) {
				return outType4{in.ValueOut3 + "<outType4>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = Run[outType4](
			ctx,
			ngn,
			inType1{"<inType>"},
		)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		assertErr(t, err, "<error>")
	})

	t.Run("should return an error before function execution if multiple inputs of the same type are provided", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(in inType1) outType1 {
				panic("should not be called")
			},
			func(ctx context.Context, in outType1) (outType2, error) {
				panic("should not be called")
			},
			func(ctx context.Context, in outType2) (outType3, error) {
				panic("should not be called")
			},
			func(ctx context.Context, in outType3) (outType4, error) {
				panic("should not be called")
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = Run[outType4](
			ctx,
			ngn,
			inType1{"<inType11>"},
			inType1{"<inType12>"},
		)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		if expected := "duplicate provided input type: warp_test.inType1"; err.Error() != expected {
			t.Fatalf("expected error message '%s', got '%s'", expected, err)
		}
	})

	t.Run("should return an error if a type of provided inputs matches a function output type", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(in inType1) outType1 {
				panic("should not be called")
			},
			func(ctx context.Context, in outType1) (outType2, error) {
				panic("should not be called")
			},
			func(ctx context.Context, in outType2) (outType3, error) {
				panic("should not be called")
			},
			func(ctx context.Context, in outType3) (outType4, error) {
				panic("should not be called")
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = Run[outType4](
			ctx,
			ngn,
			inType1{"<inType>"},
			outType1{"<outType1>"},
		)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		if expected := "provided input type matches function output type: warp_test.outType1"; err.Error() != expected {
			t.Fatalf("expected error message '%s', got '%s'", expected, err)
		}
	})

	t.Run("should not execute downstream function if an upstream function did not run", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int32
		ngn, err := Initialize(
			func(_ context.Context, in outType2, in2 outType1) (outType4, error) {
				count.Add(1)
				return outType4{in.ValueOut2 + in2.ValueOut1 + "<outType4>"}, nil
			},
			func(in outType1, in2 inType1) outType2 {
				count.Add(1)
				return outType2{in.ValueOut1 + in2.ValueIn1 + "<outType2>"}
			},
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			// this function should not be executed
			func(_ context.Context, in inType2) (outType3, error) {
				count.Add(1)
				return outType3{in.ValueIn2 + "<outType3>"}, nil
			},
			// this function should not be executed
			func(_ context.Context, in outType3) (outType5, error) {
				count.Add(1)
				return outType5{in.ValueOut3 + "<outType5>"}, nil
			},
			// this function should not be executed
			func(_ context.Context, in outType5) (outType6, error) {
				count.Add(1)
				return outType6{in.ValueOut5 + "<outType6>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType4](
			ctx,
			ngn,
			inType1{"<inType1>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 3 {
			t.Fatalf("expected 3 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}
		if expected := "<inType1><outType1><inType1><outType2><inType1><outType1><outType4>"; out[0].ValueOut4 != expected {
			t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
		}
	})

	t.Run("should allow arguments and return values in any order", func(t *testing.T) {
		t.Parallel()
		var count atomic.Int32
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			func(in outType1, in2 inType2, _ context.Context) (error, outType2) {
				count.Add(1)
				return nil, outType2{in.ValueOut1 + in2.ValueIn2 + "<outType2>"}
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType2](
			ctx,
			ngn,
			inType1{"<inType1>"},
			inType2{"<inType2>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 2 {
			t.Fatalf("expected 2 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}
		if expected := "<inType1><outType1><inType2><outType2>"; out[0].ValueOut2 != expected {
			t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
		}
	})

	t.Run("should run functions concurrently", func(t *testing.T) {
		t.Parallel()
		start := time.Now()
		var count atomic.Int32
		ngn, err := Initialize(
			func(in inType1) outType1 {
				count.Add(1)
				time.Sleep(1 * time.Second)
				return outType1{in.ValueIn1 + "<outType1>"}
			},
			func(ctx context.Context, in2 inType2) (outType2, error) {
				count.Add(1)
				time.Sleep(1 * time.Second)
				return outType2{in2.ValueIn2 + "<outType2>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := Run[outType2](
			ctx,
			ngn,
			inType1{"<inType1>"},
			inType2{"<inType2>"},
		)
		if err != nil {
			t.Fatal(err)
		}

		if count.Load() != 2 {
			t.Fatalf("expected 2 function calls, got %d", count.Load())
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 output value, got %d", len(out))
		}
		if expected := "<inType2><outType2>"; out[0].ValueOut2 != expected {
			t.Fatalf("expected output value '%s', got '%s'", expected, out[0])
		}

		dur := time.Since(start)
		if dur > 2*time.Second {
			t.Fatalf("expected execution time to be less than 2 seconds, got %s", dur)
		}
	})

	t.Run("should return context cancelled error if context is cancelled", func(t *testing.T) {
		t.Parallel()
		ngn, err := Initialize(
			func(ctx context.Context, in inType1) (outType1, error) {
				// block idefinitely
				<-ctx.Done()
				return outType1{in.ValueIn1 + "<outType1>"}, nil
			},
			func(ctx context.Context, in outType1) (outType2, error) {
				return outType2{in.ValueOut1 + "<outType2>"}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_, err = Run[outType2](
			ctx,
			ngn,
			inType1{"<inType>"},
		)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		assertErr(t, err, "context deadline exceeded")
	})
}

func assertErr(t *testing.T, actual error, expected string) {
	t.Helper()

	if actual == nil {
		t.Fatal("expected error but got nil")
	}

	if actual.Error() != expected {
		t.Fatalf("expected error message '%s', got '%s'", expected, actual)
	}
}

func assertErrContains(t *testing.T, actual error, expected string) {
	t.Helper()

	if actual == nil {
		t.Fatal("expected error but got nil")
	}

	if !strings.Contains(actual.Error(), expected) {
		t.Fatalf("expected error message '%s', got '%s'", expected, actual)
	}
}

var stringSliceTransformer = cmp.Transformer(
	"Outputs",
	func(
		in []string,
	) []string {
		// Copy input to avoid mutating it.
		out := append(
			[]string{},
			in...,
		)

		sort.SliceStable(
			out,
			func(i, j int) bool {
				return out[i] > out[j]
			},
		)

		return out
	},
)
