package vsync

import "sync"

type CheckedMap struct {
	names map[string]interface{}
	l     sync.Mutex
}

func NewCheckedMap() *CheckedMap {
	return &CheckedMap{
		names: make(map[string]interface{}),
	}
}
func (cm *CheckedMap) Lock(name string, i interface{}) bool {
	cm.l.Lock()
	defer cm.l.Unlock()
	_, ok := cm.names[name]
	if ok {
		return false
	} else {
		cm.names[name] = i
		return true
	}
}

func (cm *CheckedMap) Unlock(name string) {
	cm.l.Lock()
	defer cm.l.Unlock()
	delete(cm.names, name)
}
