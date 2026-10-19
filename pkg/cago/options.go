package cago

import "github.com/wakonig/cago/internal/ca"

// Options configures discovery, operation deadlines, array handling, and metrics.
type Options = ca.Options

// SearchTransport selects UDP discovery or TCP nameservers.
type SearchTransport = ca.SearchTransport

const (
	UDP = ca.UDP
	TCP = ca.TCP
)

// ChannelOption configures a channel handle.
type ChannelOption = ca.ChannelOption

// RequestOption configures a read or subscription.
type RequestOption = ca.RequestOption

// WithPriority selects a CA channel priority from zero through 99.
func WithPriority(priority uint16) ChannelOption { return ca.WithPriority(priority) }

// WithType selects a returned data type; Native follows the channel's native type.
func WithType(typ DataType) RequestOption { return ca.WithType(typ) }

// WithEnumType converts native ENUMs while leaving other native types unchanged.
// It is resolved after reconnects; an explicit WithType takes precedence.
func WithEnumType(typ DataType) RequestOption { return ca.WithEnumType(typ) }

// WithCount selects an element count. Zero requests the current length on CA 4.13.
func WithCount(count uint32) RequestOption { return ca.WithCount(count) }

// WithMetadata selects the metadata returned with values.
func WithMetadata(level MetadataLevel) RequestOption { return ca.WithMetadata(level) }

// WithMask selects subscription event classes.
func WithMask(mask EventMask) RequestOption { return ca.WithMask(mask) }

// WithBuffer sets a subscription consumer's local value queue capacity.
func WithBuffer(count int) RequestOption { return ca.WithBuffer(count) }
