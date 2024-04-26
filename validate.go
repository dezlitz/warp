package warp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// early engine init per function validation steps

func validateAtLeastOneFunction(fns ...any) error {
	if len(fns) == 0 {
		return errors.New("engine must be initialized with at least one function")
	}
	return nil
}

func validateTypeFunction(fnT reflect.Type) error {
	if fnT.Kind() != reflect.Func {
		return errors.New("all inputs must be functions")
	}
	return nil
}

func validateFunctionHasOutputs(fnT reflect.Type) error {
	if fnT.NumOut() == 0 {
		return errors.New("must not have no return type(s)")
	}
	return nil
}

func validateFunctionHasReturnsAtMostOneError(fnT reflect.Type) error {
	var count int
	for _, outT := range outputs(fnT) {
		if isType[error](outT) {
			count++
		}
	}
	if count > 1 {
		return errors.New("must have no more than 1 error return type")
	}

	return nil
}

func validateFunctionHasAtLeastOneNonErrorValueOutput(fnT reflect.Type) error {
	var count int
	for _, o := range outputs(fnT) {
		if !isType[error](o) {
			count++
		}
	}
	if count == 0 {
		return errors.New("must have at least 1 return value type (excluding error)")
	}

	return nil
}

func validateFunctionInputsNotError(fnT reflect.Type) error {
	for _, i := range inputs(fnT) {
		if isType[error](i) {
			return errors.New("must not have input param(s) of type error")
		}
	}
	return nil
}

func validateFunctionOutputsNotContext(fnT reflect.Type) error {
	for _, outT := range outputs(fnT) {
		if isType[context.Context](outT) {
			return errors.New("must not have any context.Context return value type(s)")
		}
	}
	return nil
}

func validateDistinctInputOutputTypes(fnT reflect.Type) error {
	for _, outT := range outputs(fnT) {
		if isType[error](outT) {
			continue
		}
		outTU, _ := unwrapOptional(outT)

		for _, inT := range inputs(fnT) {
			inTU, _ := unwrapOptional(inT)
			if outTU == inTU {
				return fmt.Errorf("input type %s is also an output type", inTU)
			}
		}
	}

	return nil
}

func validateFunctionNotVariadic(fnT reflect.Type) error {
	if fnT.Kind() == reflect.Func && fnT.IsVariadic() {
		return errors.New("must not be a variadic function")
	}
	return nil
}

func validateSameInputTypes(fnT reflect.Type) error {
	in := make(map[reflect.Type]bool, fnT.NumIn())
	for _, inT := range inputs(fnT) {
		inT, _ = unwrapOptional(inT)
		if in[inT] {
			return fmt.Errorf("function takes the same parameter type %s more than once", inT)
		}
		in[inT] = true
	}

	return nil
}

// late engine init cross-function validation steps

func validateOutputTypesUnique(fns ...any) error {
	outTypes := make(map[reflect.Type][]reflect.Value, len(fns))
	for _, fn := range fns {
		fnV := reflect.ValueOf(fn)
		for _, outT := range outputs(fnV.Type()) {
			if isType[error](outT) {
				continue
			}
			outTypes[outT] = append(outTypes[outT], fnV)
		}
	}

	for outT, providerTs := range outTypes {
		if len(providerTs) > 1 {
			badProviderRefs := strings.Join(sliceConvert(referTo, providerTs), " AND ")
			return fmt.Errorf("output value type %s already provided to the engine by %s", outT, badProviderRefs)
		}
	}

	return nil
}

func validateNoCyclicDependancies(fnVs []reflect.Value) error {
	for _, fnV := range fnVs {
		if err := checkCyclicDependancies(fnV, []reflect.Value{}, fnVs); err != nil {
			return err
		}
	}

	return nil
}

func checkCyclicDependancies(fnV reflect.Value, pathFuncs []reflect.Value, fnVs []reflect.Value) error {
	fnT := reflect.TypeOf(fnV.Interface())
	for _, pathFn := range pathFuncs {
		if pathFn.Type() == fnT {
			return fmt.Errorf("cyclic dependency detected: %s", cyclicDependencyPath(pathFuncs))
		}
	}

	pathFuncs = append(pathFuncs, fnV)

	for _, outT := range outputs(fnT) {
		if isType[error](outT) {
			continue
		}
		outTU, _ := unwrapOptional(outT)

		for _, fnV := range fnVs {
			fnT := reflect.TypeOf(fnV.Interface())
			for _, inT := range inputs(fnT) {
				inTU, _ := unwrapOptional(inT)
				if inTU == outTU {
					err := checkCyclicDependancies(fnV, pathFuncs, fnVs)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func cyclicDependencyPath(pathFuncs []reflect.Value) string {
	var path strings.Builder
	for i, fnV := range pathFuncs {
		if i > 0 {
			path.WriteString(" -> ")
		}
		path.WriteString(referTo(fnV))
	}
	return path.String()
}
