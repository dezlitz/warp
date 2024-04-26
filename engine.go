package warp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Engine is used to run a set of functions in the correct order and gather the output.
type Engine struct {
	functions   map[reflect.Type]runFunc
	outputTypes map[reflect.Type]bool
	initialized bool
}

// Initialize returns a new Engine. It validates the functions and their
// dependencies based on the following rules:
//
// * each function MUST:
//   - be of type function.
//   - return at least one non error output.
//   - return at most one error output.
//   - NOT accept an error type parameter.
//   - NOT return a context.Context type output.
//   - NOT output any types that overlap with the function parameter types
//   - NOT accept variadic parameters
//   - NOT repeat paramater types
//
// * all functions MUST:
//   - NOT have overlapping output types.
//   - NOT contain cyclic dependencies between function inputs and outputs
func Initialize(fns ...any) (engine *Engine, err error) {
	var (
		fnVs []reflect.Value
		out  = map[reflect.Type]bool{}
	)

	if err := validateAtLeastOneFunction(fns...); err != nil {
		return nil, wrapValidationError(err)
	}

	for _, fn := range fns {
		fnV := reflect.ValueOf(fn)
		fnT := reflect.TypeOf(fn)

		for _, validator := range []func(reflect.Type) error{
			validateTypeFunction,
			validateFunctionHasOutputs,
			validateFunctionHasAtLeastOneNonErrorValueOutput,
			validateFunctionHasReturnsAtMostOneError,
			validateFunctionInputsNotError,
			validateFunctionOutputsNotContext,
			validateDistinctInputOutputTypes,
			validateFunctionNotVariadic,
			validateSameInputTypes,
		} {
			if err := validator(fnT); err != nil {
				return nil, wrapValidationErrorWithInput(fnV, err)
			}
		}

		fnVs = append(fnVs, fnV)

		for _, outT := range outputs(fnT) {
			if !isType[error](outT) {
				out[outT] = true
			}
		}
	}

	if err := validateOutputTypesUnique(fns...); err != nil {
		return nil, wrapValidationError(err)
	}

	if err := validateNoCyclicDependancies(fnVs); err != nil {
		return nil, wrapValidationError(err)
	}

	return &Engine{
		functions:   buildRunFuncs(fns...),
		outputTypes: out,
		initialized: true,
	}, nil
}

// Run executes the engine functions in the order determined by their dependencies. It returns the output
// of each function where the type matches the generic type T, or is convertible to T.
//
// If any function returns an error, the execution is stopped and the error is returned.
//
// If the engine has not been initialized, an error is returned.
//
// If any of the provided input types are duplicated or match any of the function output types,
// an error is returned.
//
// If the engine cannot provide a value for a function input from either provided inputs or
// returned function values, the functions execution is skipped.
func Run[T any](ctx context.Context, e *Engine, provided ...any) ([]T, error) {
	if e == nil || !e.initialized {
		return nil, errors.New("error running engine that has not been initialized")
	}

	// Validate provided inputs
	err := validateProvided(provided, e.outputTypes)
	if err != nil {
		return nil, err
	}

	// Initialize storage with provided inputs
	storage := &sync.Map{}
	for _, in := range provided {
		inT := reflect.TypeOf(in)
		inTU, _ := unwrapOptional(inT)
		storage.Store(inTU, reflect.ValueOf(in))
	}

	// Initialize a channel for each output type
	notifiers := map[reflect.Type]chan struct{}{}
	for outT := range e.outputTypes {
		outTU, _ := unwrapOptional(outT)
		notifiers[outTU] = make(chan struct{})
	}

	eg, ctx := errgroup.WithContext(ctx)
	for _, fn := range e.functions {
		eg.Go(fn(ctx, storage, notifiers))
	}

	// Wait for all functions to complete
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Collect outputs
	var out []T
	storage.Range(func(_ any, val any) bool {
		valV := val.(reflect.Value)
		valT := valV.Type()
		valTU, _ := unwrapOptional(valT)
		if e.outputTypes[valTU] {
			if v, ok := convert[T](valV); ok {
				out = append(out, v)
			}
		}
		return true
	})
	return out, nil
}

type runFunc = func(ctx context.Context, storage *sync.Map, notifiers map[reflect.Type]chan struct{}) func() error

func buildRunFuncs(fns ...any) map[reflect.Type]runFunc {
	out := make(map[reflect.Type]runFunc, len(fns))
	for _, fn := range fns {
		fnV := reflect.ValueOf(fn)
		fnT := reflect.TypeOf(fn)
		inputs := inputs(fnT)
		outputs := outputs(fnT)
		// Get position of context input, -1 if none
		ctxPos := getPosOfType[context.Context](inputs)
		// Get position of error output, -1 if none
		errPos := getPosOfType[error](outputs)

		out[fnT] = func(ctx context.Context, storage *sync.Map, notifiers map[reflect.Type]chan struct{}) func() error {
			return func() error {
				// NOTE: anything in this func happens at runtime
				ins := make([]reflect.Value, 0, len(inputs))
				for i, inT := range inputs {
					if i == ctxPos {
						ins = append(ins, reflect.ValueOf(ctx))
						continue
					}

					if err := waitForSignal(ctx, notifiers, inT); err != nil {
						return err
					}

					// Find the value in storage
					v, ok := loadValue(storage, inT)
					if !ok {
						// Skip function if input is not available
						closeNotifiers(notifiers, outputs...)
						return nil
					}
					ins = append(ins, v)
				}

				outValues := fnV.Call(ins)
				if err := getError(outValues, errPos); err != nil {
					return err
				}

				storeOutputs(storage, outValues, outputs)

				closeNotifiers(notifiers, outputs...)

				return nil
			}
		}
	}
	return out
}

func getError(outValues []reflect.Value, errPos int) error {
	if errPos != -1 {
		if e := outValues[errPos]; !e.IsNil() {
			return e.Interface().(error)
		}
	}
	return nil
}

func storeOutputs(storage *sync.Map, outValues []reflect.Value, outputs []reflect.Type) {
	for i, outT := range outputs {
		if !isType[error](outT) {
			outTU, _ := unwrapOptional(outT)
			storage.Store(outTU, outValues[i])
		}
	}
}

func closeNotifiers(notifiers map[reflect.Type]chan struct{}, outputs ...reflect.Type) {
	for _, outT := range outputs {
		if !isType[error](outT) {
			outTU, _ := unwrapOptional(outT)
			close(notifiers[outTU])
		}
	}
}

func convert[T any](v reflect.Value) (T, bool) {
	var zero T
	// Output on exact type match
	if newV, ok := v.Interface().(T); ok {
		return newV, true
	}

	// Output on convertible type match
	if v.Type().ConvertibleTo(reflect.TypeOf((*T)(nil)).Elem()) {
		if vv, ok := v.Convert(reflect.TypeOf(zero)).Interface().(T); ok {
			return vv, true
		}
	}

	return zero, false
}

func inputs(fn reflect.Type) []reflect.Type {
	out := make([]reflect.Type, fn.NumIn())
	for i := 0; i < fn.NumIn(); i++ {
		out[i] = fn.In(i)
	}
	return out
}

func outputs(fn reflect.Type) []reflect.Type {
	out := make([]reflect.Type, fn.NumOut())
	for i := 0; i < fn.NumOut(); i++ {
		out[i] = fn.Out(i)
	}
	return out
}

// waitForSignal blocks if inT is available in the notifiers map,
// it waits until it gets notified or the context is canceled.
func waitForSignal(
	ctx context.Context,
	notifiers map[reflect.Type]chan struct{},
	inT reflect.Type,
) error {
	inTU, _ := unwrapOptional(inT)
	if _, ok := notifiers[inTU]; !ok {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-notifiers[inTU]:
		return nil
	}
}

func loadValue(
	storage *sync.Map,
	inT reflect.Type,
) (_ reflect.Value, ok bool) {
	// Unwrap function input type if it is Optional[T]
	inTU, isInTOptional := unwrapOptional(inT)

	// Load value from storage
	v, ok := storage.Load(inTU)
	if !ok {
		// Return zero value if input is not available and allow function to run
		if isInTOptional {
			return reflect.Zero(inT), true
		}

		// Skip function if input is not available and not Optional[T]
		return reflect.Value{}, false
	}

	// Wrap value in Optional[T] if function input type is Optional[T] and value is NOT also Optional[T]
	if isInTOptional && v.(reflect.Value).Type() != inT {
		return newOptional(inT, v.(reflect.Value)), true
	}

	// if function input type is T and value is Optional[T]
	if !isInTOptional && isOptional(v.(reflect.Value).Type()) {
		if v.(reflect.Value).FieldByName("IsSet").Bool() {
			// Unwrap value
			return v.(reflect.Value).FieldByName("Val"), true
		}
		// Skip function if input is Optional but not set
		return reflect.Value{}, false
	}

	// Both input type and value are Optional[T]
	if isInTOptional && v.(reflect.Value).Type() == inT {
		// Set value to empty if Optional[T] is not set
		if !v.(reflect.Value).FieldByName("IsSet").Bool() {
			return reflect.Zero(inT), true
		}
		// Unwrap value
		return v.(reflect.Value).FieldByName("Val"), true
	}

	return v.(reflect.Value), true
}

func wrapValidationErrorWithInput(badInput reflect.Value, err error) error {
	return fmt.Errorf("input %s caused validation error: %w", referTo(badInput), err)
}

func wrapValidationError(err error) error {
	return fmt.Errorf("input validation error: %w", err)
}

func referTo(rv reflect.Value) string {
	rvT := rv.Type()
	rvtU, _ := unwrapOptional(rvT)
	reference := rvtU.String()
	if rv.Type().Kind() == reflect.Func {
		reference = strings.TrimPrefix(reference, "func")              // remove generic func type prefix
		reference = runtime.FuncForPC(rv.Pointer()).Name() + reference // make func name the prefix
	}
	return reference
}

func isType[T any](in reflect.Type) bool {
	needle := reflect.TypeOf((*T)(nil)).Elem()
	return in == needle
}

func sliceConvert[T any, V any](f func(T) V, in []T) []V {
	out := make([]V, len(in))
	for i := range in {
		out[i] = f(in[i])
	}
	return out
}

func getPosOfType[T any](in []reflect.Type) int {
	for i, t := range in {
		if isType[T](t) {
			return i
		}
	}
	return -1
}

func validateProvided(provided []any, outputs map[reflect.Type]bool) error {
	// Unwrap any Optional[T] output types
	outputsU := map[reflect.Type]bool{}
	for outT := range outputs {
		outTU, _ := unwrapOptional(outT)
		outputsU[outTU] = true
	}

	checked := map[reflect.Type]bool{}
	for _, in := range provided {
		inT := reflect.TypeOf(in)
		inTU, _ := unwrapOptional(inT)
		if alreadyChecked := checked[inT]; alreadyChecked {
			return fmt.Errorf("duplicate provided input type: %s", inTU)
		}

		if outputsU[inTU] {
			return fmt.Errorf("provided input type matches function output type: %s", inTU)
		}

		checked[inT] = true
	}
	return nil
}

// newOptional constructs a new Optional[T] type with the provided type and value.
func newOptional(t reflect.Type, v reflect.Value) reflect.Value {
	if !isOptional(t) {
		panic(fmt.Sprintf("type %s is not Optional[T] type", t.Name()))
	}

	val := reflect.New(t)

	val.Elem().FieldByName("Val").Set(v)
	val.Elem().FieldByName("IsSet").SetBool(true)

	return val.Elem()
}
