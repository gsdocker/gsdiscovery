package zk

import (
	"bytes"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/gsdocker/gsconfig"
	"github.com/gsdocker/gsdiscovery"
	"github.com/gsdocker/gserrors"
	"github.com/gsdocker/gslogger"
	"github.com/gsrpc/gorpc"
	"github.com/samuel/go-zookeeper/zk"
)

type _Watcher struct {
	gslogger.Log                                // watcher log
	conn         *zk.Conn                       // zookeeper connection
	client       *Client                        // zookeeper client
	events       chan *gsdiscovery.Event        // events queue
	path         string                         // watch znode path
	cached       map[string]*gorpc.NamedService // cached children
}

func newWatcher(path string, client *Client, children []string, events <-chan zk.Event) gsdiscovery.Watcher {
	watcher := &_Watcher{
		Log:    gslogger.Get("gsdiscovery-zk"),
		conn:   client.conn,
		client: client,
		events: make(chan *gsdiscovery.Event, gsconfig.Int("gsdisconvery.zk.events.size", 1024)),
		path:   path,
		cached: make(map[string]*gorpc.NamedService),
	}

	watcher.fireEvent(children)

	go func() {

		for {
			select {
			case event, ok := <-events:

				if !ok {
					watcher.V("watcher(%s) stopped ...")
					return
				}

				watcher.D("znode(%s) event %s", event.Path, event.Type)

				if zk.EventNodeChildrenChanged != event.Type {
					continue
				}

				var err error

				children, _, events, err = watcher.conn.ChildrenW(path)

				if err != nil {
					watcher.Close()
					break
				}

				watcher.fireEvent(children)
			}
		}

	}()

	return watcher
}

func (watcher *_Watcher) fireEvent(children []string) {

	var created []*gorpc.NamedService
	var deleted []*gorpc.NamedService

	cached := make(map[string]*gorpc.NamedService)

	for _, child := range children {

		zkpath := path.Join(watcher.path, child)

		if service, ok := watcher.getData(zkpath); ok {
			cached[zkpath] = service

			if _, ok := watcher.cached[zkpath]; !ok {
				created = append(created, service)
			}
		}
	}

	for zkpath, service := range watcher.cached {
		if _, ok := cached[zkpath]; !ok {
			deleted = append(deleted, service)
		}
	}

	watcher.cached = cached

	if len(created) != 0 {
		watcher.events <- &gsdiscovery.Event{Services: created, State: gsdiscovery.EvtCreated}

		watcher.D("watcher(%s) fire EvtCreated", watcher.path)
	}

	if len(deleted) != 0 {
		watcher.events <- &gsdiscovery.Event{Services: deleted, State: gsdiscovery.EvtDeleted}

		watcher.D("watcher(%s) fire EvtDeleted", watcher.path)
	}

}

func (watcher *_Watcher) getData(zkpath string) (*gorpc.NamedService, bool) {
	watcher.D("get znode :%s content", zkpath)
	content, _, events, err := watcher.conn.GetW(zkpath)

	if err != nil {
		watcher.E("get znode %s content error\n\t%s", zkpath, err)
		return nil, false
	}

	watcher.D("get znode :%s content -- success", zkpath)

	namedService, err := gorpc.ReadNamedService(bytes.NewBuffer(content))

	if err != nil {
		watcher.E("unmarshal znode %s content error\n\t%s", zkpath, err)
		return nil, false
	}

	go watcher.watchData(events, zkpath)

	return namedService, true
}

func (watcher *_Watcher) watchData(events <-chan zk.Event, zkpath string) {
	for {
		select {
		case event, ok := <-events:

			if !ok {
				watcher.V("watcher(%s) stopped ...", watcher.path)
				return
			}

			watcher.D("znode(%s) event %s", event.Path, event.Type)

			if zk.EventNodeDataChanged != event.Type {
				continue
			}

			var err error

			var content []byte

			content, _, events, err = watcher.conn.GetW(zkpath)

			if err != nil {
				break
			}

			namedService, err := gorpc.ReadNamedService(bytes.NewBuffer(content))

			if err != nil {
				watcher.E("unmarshal znode %s content error\n\t%s", zkpath, err)
				continue
			}

			watcher.events <- &gsdiscovery.Event{Services: []*gorpc.NamedService{namedService}, State: gsdiscovery.EvtUpdated}
		}
	}
}

func (watcher *_Watcher) Close() {
	close(watcher.events)

	watcher.client.closeWatcher(watcher)
}

func (watcher *_Watcher) Chan() <-chan *gsdiscovery.Event {
	return watcher.events
}

// Client the gsdiscovery provider using zookeeper protocol
type Client struct {
	sync.Mutex // mixin mutex
	log        gslogger.Log
	conn       *zk.Conn                       // zookeeper connection
	path       string                         // service watch parent path
	watchers   map[string]gsdiscovery.Watcher // register service watchers
}

// New create new gsdiscovery which's backend is zookeeper
func New(servers []string) (gsdiscovery.Discovery, error) {

	conn, _, err := zk.Connect(servers, gsconfig.Seconds("gsdiscovery.zk.session.timeout", 5))

	if err != nil {
		return nil, err
	}

	client := &Client{
		log:      gslogger.Get("gsdiscovery-zk"),
		conn:     conn,
		path:     path.Clean(path.Join("/", gsconfig.String("gsdiscovery.zk.watch.path", "/gsdiscovery"))),
		watchers: make(map[string]gsdiscovery.Watcher),
	}

	conn.SetLogger(client)

	return client, nil
}

// Printf implement zk.Logger
func (client *Client) Printf(f string, args ...interface{}) {
	client.log.D(fmt.Sprintf(f, args...))
}

// WatchPath the zookeeper service watch root path
func (client *Client) WatchPath(path string) gsdiscovery.Discovery {
	client.path = path
	return client
}

func (client *Client) ensureExists(zkpath string) error {
	nodes := strings.Split(zkpath, "/")

	zkpath = "/"

	for _, node := range nodes {

		zkpath = path.Join(zkpath, node)

		if zkpath == "/" {
			continue
		}

		client.log.D("check zk path %s", zkpath)

		ok, _, err := client.conn.Exists(zkpath)

		if err != nil {
			return err
		}

		if !ok {
			_, err := client.conn.Create(zkpath, nil, 0, zk.WorldACL(zk.PermAll))

			if err != nil {
				return gserrors.Newf(err, "create znode :%s error", zkpath)
			}
		}
	}

	return nil
}

func (client *Client) closeWatcher(watcher *_Watcher) {
	client.Lock()
	defer client.Unlock()

	delete(client.watchers, fmt.Sprintf("%p", watcher))
}

// Watch create new zk node watcher
func (client *Client) Watch(name string) (gsdiscovery.Watcher, error) {

	path := fmt.Sprintf("%s/%s", client.path, name)

	if err := client.ensureExists(path); err != nil {
		return nil, err
	}

	client.log.D("watch service znode :%s", path)

	children, _, events, err := client.conn.ChildrenW(path)

	client.Lock()
	defer client.Unlock()

	watcher := newWatcher(path, client, children, events)

	client.watchers[fmt.Sprintf("%p", watcher)] = watcher

	return watcher, err
}

// Register register new named services to zk servers
func (client *Client) Register(name *gorpc.NamedService) error {

	var buff bytes.Buffer

	err := gorpc.WriteNamedService(&buff, name)

	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s/%s", client.path, name.Name)

	client.ensureExists(path)

	path = fmt.Sprintf("%s/%s", path, name.NodeName)

	for {
		_, err = client.conn.Create(path, buff.Bytes(), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))

		if err != nil && err != zk.ErrNodeExists {
			return gserrors.Newf(err, "create znode :%s error", path)
		}

		if err == zk.ErrNodeExists {
			client.log.D("update znode :%s", path)

			_, err := client.conn.Set(path, buff.Bytes(), 0)

			if err == zk.ErrNoNode {
				continue
			}

			if err != nil {
				return gserrors.Newf(err, "create znode :%s error", path)
			}
		}

		client.log.D("register znode :%s -- success", path)

		return nil
	}

}

// Close implement the Discovery interface
func (client *Client) Close() {

	client.Lock()
	defer client.Unlock()

	for _, watcher := range client.watchers {
		watcher.Close()
	}
}

// Unregister unregister new named services to zk servers
func (client *Client) Unregister(name *gorpc.NamedService) error {

	path := fmt.Sprintf("%s/%s/%s", client.path, name.Name, name.NodeName)

	client.log.D("unregister znode :%s", path)

	_, state, err := client.conn.Get(path)

	if err == zk.ErrNoNode {
		return nil
	}

	if err != nil {
		return gserrors.Newf(err, "unregister znode :%s -- failed", path)
	}

	err = client.conn.Delete(path, state.Version)

	if err == zk.ErrNoNode {
		return nil
	}

	if err != nil {
		return gserrors.Newf(err, "unregister znode :%s -- failed", path)
	}

	client.log.D("unregister znode :%s -- success", path)

	return nil
}
