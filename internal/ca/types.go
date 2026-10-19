// Package ca implements the private Channel Access runtime.
package ca

import (
	"errors"
	"fmt"
	"time"
)

type DataType uint16

const (
	String DataType = iota
	Short
	Float
	Enum
	Char
	Long
	Double
)
const (
	PutACKT      DataType = 35
	PutACKS      DataType = 36
	STSACKString DataType = 37
	ClassName    DataType = 38
	Native       DataType = 65535
)

func (t DataType) Base() DataType {
	if t <= 34 {
		return t % 7
	}
	switch t {
	case PutACKT, PutACKS:
		return Enum
	case STSACKString, ClassName:
		return String
	}
	return Native
}
func (t DataType) Valid() bool { return t <= 38 }
func (t DataType) Size() int {
	switch t.Base() {
	case String:
		return 40
	case Short, Enum:
		return 2
	case Char:
		return 1
	case Float, Long:
		return 4
	case Double:
		return 8
	}
	return 0
}

type MetadataLevel uint16

const (
	Plain MetadataLevel = iota
	Status
	Time
	Graphics
	Control
)

type Metadata struct {
	Level            MetadataLevel
	Status, Severity uint16
	Timestamp        time.Time
	// SecondsPastEpoch and Nanoseconds preserve the original EPICS timestamp.
	SecondsPastEpoch, Nanoseconds uint32
	// EnumCount is the original signed DBR enum label count.
	EnumCount                                    int16
	Precision                                    int16
	Units                                        string
	DisplayHigh, DisplayLow                      float64
	AlarmHigh, WarningHigh, WarningLow, AlarmLow float64
	ControlHigh, ControlLow                      float64
	EnumLabels                                   []string
	ACKTransient, ACKSeverity                    uint16
}

type AccessRights uint32

const (
	ReadAccess  AccessRights = 1
	WriteAccess AccessRights = 2
)

type State uint8

const (
	Searching State = iota
	Connected
	Closed
)

func (s State) String() string {
	switch s {
	case Searching:
		return "searching"
	case Connected:
		return "connected"
	case Closed:
		return "closed"
	}
	return "unknown"
}

type ChannelInfo struct {
	Name, Host string
	State      State
	NativeType DataType
	Count      uint32
	Access     AccessRights
	Priority   uint16
}
type StateChange struct {
	Info ChannelInfo
	Err  error
	At   time.Time
}
type Event struct {
	PV         string
	Value      Value
	ReceivedAt time.Time
}
type EventMask uint16

const (
	ValueEvent    EventMask = 1
	LogEvent      EventMask = 2
	AlarmEvent    EventMask = 4
	PropertyEvent EventMask = 8
)

var (
	ErrCreateRejected = errors.New("CA channel creation rejected")
	ErrClosed         = errors.New("CA handle closed")
	ErrDisconnected   = errors.New("CA channel disconnected")
	ErrBadType        = errors.New("invalid CA data type")
	ErrBadCount       = errors.New("invalid CA element count")
	ErrNoRead         = errors.New("CA read access denied")
	ErrNoWrite        = errors.New("CA write access denied")
	ErrIDExhausted    = errors.New("CA channel identifier space exhausted; create a new client")
)

// ProtocolError retains the full CA status and triggering request command.
type ProtocolError struct {
	Status  uint32
	Command uint16
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("CA status %d on command %d: %s", e.Status, e.Command, e.Message)
}
func statusError(status uint32, command uint16) error {
	if status == 1 {
		return nil
	}
	return &ProtocolError{Status: status, Command: command, Message: "server rejected request"}
}

// Unwrap makes standard server rejections usable with errors.Is while retaining
// the original protocol status for diagnostics.
func (e *ProtocolError) Unwrap() error {
	switch (e.Status >> 3) & 0x1fff {
	case 14:
		return ErrBadType
	case 22:
		return ErrBadCount
	case 24:
		return ErrDisconnected
	case 46:
		return ErrNoRead
	case 47:
		return ErrNoWrite
	}
	return nil
}
