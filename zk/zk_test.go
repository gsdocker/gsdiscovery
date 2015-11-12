package zk

import (
	"testing"

	"github.com/gsdocker/gslogger"
	"github.com/gsrpc/gorpc"

	"github.com/gsdocker/gsdiscovery"
)

var discovery gsdiscovery.Discovery

var log = gslogger.Get("test")

func TestConnect(t *testing.T) {
	var err error
	discovery, err = New([]string{"10.0.0.213:2181"})

	if err != nil {
		t.Fatal(err)
	}
}

func TestWatch(t *testing.T) {

	watcher, err := discovery.Watch("test")

	if err != nil {
		t.Fatal(err)
	}

	namedService := gorpc.NewNamedService()

	namedService.Name = "test"

	namedService.NodeName = "127.0.0.1:13512"

	namedService.VNodes = 1

	err = discovery.Register(namedService)

	if err != nil {
		t.Fatal(err)
	}

	event := <-watcher.Chan()

	if event.State != gsdiscovery.EvtCreated {
		t.Fatalf("check data changed event error")
	}

	namedService.VNodes = 10

	err = discovery.Register(namedService)

	if err != nil {
		t.Fatal(err)
	}

	event = <-watcher.Chan()

	if len(event.Services) != 1 {
		t.Fatalf("check data changed event error")
	}

	err = discovery.Unregister(namedService)

	if err != nil {
		t.Fatal(err)
	}

	event = <-watcher.Chan()

	if event.State != gsdiscovery.EvtDeleted {
		t.Fatalf("check data changed event error")
	}
}
