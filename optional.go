package warp

import (
	"fmt"
	"reflect"
)

// Optional is a wrapper for an optional parameter in the function run by
// engine.
type Optional[T any] struct {
	Val   T
	IsSet bool
}

func (o Optional[T]) isOptional() {}

// Value returns the value wrapped in Optional type and a boolean indicating if
// the value was set.
func (o Optional[T]) Value() (T, bool) {
	return o.Val, o.IsSet
}

type optional interface {
	isOptional()
}

// isOptional returns true if the type is an explicit Optional type.
// Custom types derived from Optional[T] are not supported.
func isOptional(t reflect.Type) bool {
	return t.Implements(reflect.TypeOf((*optional)(nil)).Elem())
}

// unwrapOptional returns the type of the value wrapped by an Optional[T]. If the value
// was not wrapped in Optional[T] then ok is false and the type is returned
// unaltered.
func unwrapOptional(t reflect.Type) (_ reflect.Type, ok bool) {
	if !isOptional(t) {
		return t, false
	}

	field, ok := t.FieldByName("Val")
	if !ok {
		panic(fmt.Sprintf("Optional type %s has no Val field", t))
	}

	return field.Type, true
}
