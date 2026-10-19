package cago

import "github.com/wakonig/cago/internal/ca"

// DataType selects the value representation returned by a channel operation.
type DataType = ca.DataType

const (
	String       = ca.String
	Short        = ca.Short
	Float        = ca.Float
	Enum         = ca.Enum
	Char         = ca.Char
	Long         = ca.Long
	Double       = ca.Double
	PutACKT      = ca.PutACKT
	PutACKS      = ca.PutACKS
	STSACKString = ca.STSACKString
	ClassName    = ca.ClassName
	Native       = ca.Native
)

// MetadataLevel selects plain, status, timestamp, graphics, or control metadata.
type MetadataLevel = ca.MetadataLevel

const (
	Plain    = ca.Plain
	Status   = ca.Status
	Time     = ca.Time
	Graphics = ca.Graphics
	Control  = ca.Control
)

// Metadata accompanies a returned Value and preserves original server fields.
type Metadata = ca.Metadata

// AccessRights describes the current read and write permissions for a channel.
type AccessRights = ca.AccessRights

const (
	ReadAccess  = ca.ReadAccess
	WriteAccess = ca.WriteAccess
)

// State describes a channel's connection state.
type State = ca.State

const (
	Searching = ca.Searching
	Connected = ca.Connected
	Closed    = ca.Closed
)

// ChannelInfo is a snapshot of channel identity, capacity, access, and connection.
type ChannelInfo = ca.ChannelInfo

// StateChange reports channel state or access changes.
type StateChange = ca.StateChange

// Event is a subscription value with its PV name and local observation time.
type Event = ca.Event

// EventMask selects the server changes that trigger subscription updates.
type EventMask = ca.EventMask

const (
	ValueEvent    = ca.ValueEvent
	LogEvent      = ca.LogEvent
	AlarmEvent    = ca.AlarmEvent
	PropertyEvent = ca.PropertyEvent
)

// Errors from operations support errors.Is with these sentinel values.
var (
	ErrCreateRejected = ca.ErrCreateRejected
	ErrClosed         = ca.ErrClosed
	ErrDisconnected   = ca.ErrDisconnected
	ErrBadType        = ca.ErrBadType
	ErrBadCount       = ca.ErrBadCount
	ErrNoRead         = ca.ErrNoRead
	ErrNoWrite        = ca.ErrNoWrite
	ErrIDExhausted    = ca.ErrIDExhausted
)

// ProtocolError preserves a server rejection's status and triggering command.
type ProtocolError = ca.ProtocolError
