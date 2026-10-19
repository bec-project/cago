package cago

import "github.com/wakonig/cago/internal/ca"

// Value owns immutable value data and metadata. Array and byte accessors return
// copies. Values are constructed with NewValue or returned by channel operations.
type Value = ca.Value

// NewValue constructs a write value from a supported Go scalar or slice. It
// rejects overflowing numbers and STRING values longer than 39 bytes.
func NewValue(input any) (Value, error) { return ca.NewValue(input) }
