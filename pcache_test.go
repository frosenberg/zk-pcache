package zkmanager

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestNewShouldWatchChildren(t *testing.T) {
	c := connect()

	p, err := c.Create(newPath("/foo1"), []byte(""), 0, zk.WorldACL(zk.PermAll))
	fmt.Println("Created: ", p)
	assert.NoError(t, err)

	pc, _, _ := NewPathCache(p, 1, c)
	ch, err := pc.Start()
	assert.NoError(t, err)
	defer pc.Stop()

	childPath := newPath(p + "/child1")
	go func() {
		c.Create(childPath, []byte("value"), 0, zk.WorldACL(zk.PermAll))
	}()

	waitForNotification(ch, PathCacheAddNotification, childPath)

	// check path value
	assert.Equal(t, []byte("value"), pc.PathValue(childPath))
	assert.Equal(t, 1, pc.NumChildren())
	assert.Equal(t, []string{childPath}, pc.Children())

	defer c.Close() // FIXME this is a bug, if i put it earlier if fails with race on pc.Stop()
}

func TestUpdateSendsNotification(t *testing.T) {
	c := connect()

	parent, err := c.Create(newPath("/foo2"), []byte(""), 0, zk.WorldACL(zk.PermAll))
	assert.NoError(t, err)
	fmt.Println("Created: ", parent)

	childPath := newPath(parent + "/child1")
	childPath, err = c.Create(childPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
	assert.NoError(t, err)
	fmt.Println("Created: ", childPath)

	pc, _, err := NewPathCache(childPath, 1, c)
	assert.NoError(t, err)
	ch, err := pc.Start()
	assert.NoError(t, err)
	defer pc.Stop()

	go func() {
		c.Set(childPath, []byte("updated_value"), 0)
	}()

	waitForNotification(ch, PathCacheUpdateNotification, childPath)

	defer c.Close() // FIXME this is a bug, if i put it earlier if fails with race on pc.Stop()
}

func TestDeleteSendsNotification(t *testing.T) {
	c := connect()

	parent, err := c.Create(newPath("/foo3"), []byte(""), 0, zk.WorldACL(zk.PermAll))
	assert.NoError(t, err)
	fmt.Println("Created: ", parent)

	pc, _, err := NewPathCache(parent, 1, c)
	assert.NoError(t, err)
	ch, err := pc.Start()
	assert.NoError(t, err)
	defer pc.Stop()

	childPath := newPath(parent + "/child1")
	go func() {
		childPath, err = c.Create(childPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
		assert.NoError(t, err)
		fmt.Println("Created: ", childPath)
	}()

	waitForNotification(ch, PathCacheAddNotification, childPath)

	go func() {
		c.Delete(childPath, -1)
	}()

	waitForNotification(ch, PathCacheRemoveNotification, childPath)

	defer c.Close() // FIXME this is a bug, if i put it earlier if fails with race on pc.Stop()
}

// 	pcs := newTestPathCache("/foo", []byte(""), 0)
// 	defer pcs.cleanup()
//
// 	pc, events, _ := pcs.PathCache()
// 	defer pc.Stop()
//
// 	pcs.updateValue([]byte("Update1"))
//
// 	// Wait for event on Path before checking value
// 	ev := <-events
//
// 	if ev.Type != PathCacheUpdateNotification || ev.Path != pcs.path {
// 		t.Fatal("EventNodeDataChanged not propogated to parents")
// 	}
// }
//
// func TestShouldUpdateValueOnChange(t *testing.T) {
// 	pcs := newTestPathCache("/foo", []byte(""), 0)
// 	defer pcs.cleanup()
//
// 	pc, pathEvents, err := pcs.PathCache()
// 	defer pc.Stop()
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	// Trying multiple times will ensure the watch is activated again
// 	values := []string{"foo", "bar", "baz"}
//
// 	for _, v := range values {
// 		pcs.pathValue = []byte(v)
// 		pcs.sendEvent(zk.EventNodeDataChanged, pcs.path)
//
// 		// Wait for event on Path before checking value
// 		<-pathEvents
// 		pcVal := pc.Value()
//
// 		if !bytes.Equal(pcVal, pcs.pathValue) {
// 			t.Fatal("PathCache did not update value:", string(pcVal), string(pcs.pathValue))
// 		}
// 	}
// }
//
// func TestDeletePathShouldNotifyParent(t *testing.T) {
// 	pcs := newTestPathCache("/foo", []byte(""), 0)
// 	defer pcs.cleanup()
//
// 	pc, events, _ := pcs.PathCache()
// 	defer pc.Stop()
//
// 	go pcs.sendEvent(zk.EventNodeDeleted, pcs.path)
//
// 	// Wait for event on Path before checking value
// 	ev := <-events
//
// 	if ev.Type != PathCacheRemoveNotification || ev.Path != pcs.path {
// 		t.Fatal("EventNodeDeleted not propogated to parents")
// 	}
// }
//
// func TestStop(t *testing.T) {
// 	pcs := newTestPathCache("/foo", []byte(""), 0)
// 	defer pcs.cleanup()
//
// 	pc, events, _ := pcs.PathCache()
// 	pc.Stop()
//
// 	_, ok := <-events
//
// 	if ok {
// 		t.Fatal("Stop should shutdown notification channel")
// 	}
//
// 	if len(pc.children) > 0 {
// 		t.Fatal("children not removed")
// 	}
// }
//
// func TestNewShouldWatchChildren(t *testing.T) {
// 	ch := make(chan bool)
// 	pcs := newTestPathCache("/foo", []byte(""), 1)
// 	defer pcs.cleanup()
//
// 	pcs.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		go func() {
// 			ch <- true
// 		}()
//
// 		return pcs.pathChildren, pcs.pathStat, pcs.pathEvents, nil
// 	}
//
// 	pc, _, _ := NewPathCache(pcs.path, pcs.depth, pcs.serviceManager)
// 	defer pc.Stop()
//
// 	if !assertChannelReceived(ch, 10*time.Millisecond) {
// 		t.Fatal("ChildrenW not called when creating PathCache with depth > 0")
// 	}
// }
//
// func TestShouldNotWatchChildrenIfDepthIsZero(t *testing.T) {
// 	ch := make(chan bool)
// 	pcs := newTestPathCache("/foo", []byte(""), 0)
// 	defer pcs.cleanup()
//
// 	pcs.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		go func() {
// 			ch <- true
// 		}()
//
// 		return pcs.pathChildren, pcs.pathStat, pcs.pathEvents, nil
// 	}
//
// 	pc, _, _ := NewPathCache(pcs.path, pcs.depth, pcs.serviceManager)
// 	defer pc.Stop()
//
// 	if assertChannelReceived(ch, 10*time.Millisecond) {
// 		t.Fatal("ChildrenW should not called when creating PathCache with depth is 0")
// 	}
// }
//
// func TestHasChildren(t *testing.T) {
// 	ch := make(chan bool)
//
// 	pcsParent := newTestPathCache("/parent", []byte(""), 1)
// 	defer pcsParent.cleanup()
//
// 	pcsParent.pathChildren = []string{"child"}
//
// 	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
// 	defer pcsChild.cleanup()
//
// 	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		if path == "/parent/child" {
// 			go func() {
// 				ch <- true
// 			}()
//
// 			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
// 		}
//
// 		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
// 	}
//
// 	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		if path == "/parent/child" {
// 			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathEvents, nil
// 		}
//
// 		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathEvents, nil
// 	}
//
// 	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
// 	defer pc.Stop()
//
// 	if err != nil {
// 		t.Error(err)
// 	}
//
// 	ev := <-events
//
// 	if ev.Type != PathCacheAddNotification || ev.Path != pcsChild.path {
// 		t.Fatal("Parent PathCache not notified of new child")
// 	}
//
// 	if !assertChannelReceived(ch, 10*time.Millisecond) {
// 		t.Fatal("GetW should be called for child")
// 	}
// }
//
// func TestDecreaseDepthWhenCreatingChild(t *testing.T) {
// 	pcsParent := newTestPathCache("/parent", []byte(""), 1)
// 	defer pcsParent.cleanup()
//
// 	pcsParent.pathChildren = []string{"child"}
//
// 	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
// 	defer pcsChild.cleanup()
//
// 	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		if path == "/parent/child" {
// 			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
// 		}
//
// 		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
// 	}
//
// 	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		if path == "/parent/child" {
// 			t.Fatal("ChildrenW should not be called on child when depth has been decreased")
//
// 			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathEvents, nil
// 		}
//
// 		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathEvents, nil
// 	}
//
// 	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
// 	defer pc.Stop()
//
// 	if err != nil {
// 		t.Error(err)
// 	}
//
// 	// Read child add
// 	<-events
//
// 	if pc.children["/parent/child"].depth != 0 {
// 		t.Fatal("Depth should be decreased for child")
// 	}
// }
//
// func TestAddChild(t *testing.T) {
// 	pcsParent := newTestPathCache("/parent", []byte(""), 1)
// 	defer pcsParent.cleanup()
//
// 	pcsParent.pathChildren = []string{"child"}
//
// 	pcsChild := newTestPathCache("/parent/child", []byte(""), 0)
// 	defer pcsChild.cleanup()
//
// 	pcsChild2 := newTestPathCache("/parent/child2", []byte(""), 0)
// 	defer pcsChild2.cleanup()
//
// 	pcsParent.zkConn.GetWFunc = func(path string) (b []byte, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		if path == "/parent/child" {
// 			return pcsChild.pathValue, pcsChild.pathStat, pcsChild.pathEvents, nil
// 		} else if path == "/parent/child2" {
// 			return pcsChild2.pathValue, pcsChild2.pathStat, pcsChild2.pathEvents, nil
// 		}
//
// 		return pcsParent.pathValue, pcsParent.pathStat, pcsParent.pathEvents, nil
// 	}
//
// 	pcsParent.zkConn.ChildrenWFunc = func(path string) (children []string, st *zk.Stat, ev <-chan zk.Event, err error) {
// 		if path == "/parent/child" {
// 			return pcsChild.pathChildren, pcsChild.pathStat, pcsChild.pathChildrenEvents, nil
// 		} else if path == "/parent/child2" {
// 			return pcsChild2.pathChildren, pcsChild2.pathStat, pcsChild2.pathChildrenEvents, nil
// 		}
//
// 		return pcsParent.pathChildren, pcsParent.pathStat, pcsParent.pathChildrenEvents, nil
// 	}
//
// 	pc, events, err := NewPathCache(pcsParent.path, pcsParent.depth, pcsParent.serviceManager)
// 	defer pc.Stop()
//
// 	if err != nil {
// 		t.Error(err)
// 	}
//
// 	// Read initial child add
// 	<-events
//
// 	// Add an additional child
// 	pcsParent.pathChildren = append(pcsParent.pathChildren, "child2")
// 	go pcsParent.sendChildEvent(zk.EventNodeChildrenChanged, pcsParent.path)
//
// 	ev := <-events
//
// 	if ev.Type != PathCacheAddNotification || ev.Path != pcsChild2.path {
// 		t.Fatal("Parent PathCache not notified of new child")
// 	}
// }
//

func waitForNotification(ch chan PathCacheNotification, typ PathCacheNotificationType, path string) {
	timeout := time.After(3 * time.Second)

	for {
		select {
		case n := <-ch:
			// log.Println("n: ", n)
			if n.Type == typ && n.Path == path {
				return
			}
		case <-timeout:
			log.Println("timeout happened")
			panic("timed out waiting for notification")
		}
	}
}

func receiveNotification(ch chan PathCacheNotification, d time.Duration) *PathCacheNotification {
	timeout := time.After(d)
	resp := make(chan *PathCacheNotification)

	go func() {
		select {
		case val := <-ch:
			fmt.Println("val: ", val)
			resp <- &val
		case <-timeout:
			resp <- nil
		}
	}()

	return <-resp
}

func connect() *zk.Conn {
	server := os.Getenv("ZK_SERVER")
	if server == "" {
		server = "localhost:2181"
	}
	conn, _, err := zk.Connect([]string{server}, 10*time.Second)
	if err != nil {
		panic(err)
	}
	return conn
}

func newPath(path string) string {
	return fmt.Sprintf("%s_%d", path, time.Now().Unix())
}
