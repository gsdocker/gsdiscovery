package gsdiscovery

import (
	"github.com/gsrpc/gorpc"
)

// Pub the gsdocker service publisher
type Pub interface {
	// Register register new named service on center service publisher
	Register(name *gorpc.NamedService) error
	// Unregister unregister named service from center service publisher
	Unregister(name *gorpc.NamedService) error
}

// Evt event state
type Evt int

// event state enum
const (
	EvtCreated = Evt(iota)
	EvtUpdated
	EvtDeleted
)

// Event Watch event
type Event struct {
	Services map[string]*gorpc.NamedService
	Updates  map[string]*gorpc.NamedService
	State    Evt
}

// Watcher the service watcher
type Watcher interface {
	// Close close current watcher
	Close()

	// Chan get watch event chan
	Chan() <-chan *Event
}

// Sub the gsdocker service subscriber
type Sub interface {
	// Start a new service watcher
	Watch(name string) (Watcher, error)
}

// Discovery the gsdocker discovery interface
type Discovery interface {
	Pub
	Sub
	WatchPath(path string) Discovery
	UpdateRegistry(path string) error
	Close()
}
