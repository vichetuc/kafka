package kafka

import (
	"errors"
	"sync"
	"time"
)

// connectionPool is a way for us to manage multiple connections to a Kafka broker in a way
// that balances out throughput with overall number of connections.
type connectionPool struct {
	conf BrokerConf

	// mu protects the below members of this struct. This mutex must only be used by
	// connectionPool.
	mu     *sync.RWMutex
	closed bool
	chans  map[string]chan *connection
	addrs  []string
}

// newConnectionPool creates a connection pool and initializes it.
func newConnectionPool(conf BrokerConf) *connectionPool {
	return &connectionPool{
		conf:  conf,
		mu:    &sync.RWMutex{},
		chans: make(map[string]chan *connection),
		addrs: make([]string, 0),
	}
}

// getAddrChan fetches a channel for a given address. This takes the read lock. If no
// channel exists, nil is returned.
func (cp *connectionPool) getAddrChan(addr string) chan *connection {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		return nil
	}

	if _, ok := cp.chans[addr]; !ok {
		return nil
	}
	return cp.chans[addr]
}

// getOrCreateAddrChan fetches a channel for a given address and, if one doesn't exist,
// creates it. This function takes the write lock against the pool.
func (cp *connectionPool) getOrCreateAddrChan(addr string) chan *connection {
	// Fast path: only gets a read lock
	chn := cp.getAddrChan(addr)
	if chn != nil {
		return chn
	}

	// Did not exist, take the slow path
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return nil
	}

	if _, ok := cp.chans[addr]; !ok {
		cp.addrs = append(cp.addrs, addr)
		cp.chans[addr] = make(chan *connection, 10)
	}
	return cp.chans[addr]
}

// GetAllAddrs returns a slice of all addresses we've seen. Can be used for picking a random
// address or iterating the known brokers.
func (cp *connectionPool) GetAllAddrs() []string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	ret := make([]string, len(cp.addrs))
	copy(ret, cp.addrs)
	return ret
}

// InitializeAddrs takes in a set of addresses and just sets up the structures for them. This
// doesn't start any connecting. This is done so that we have a set of addresses for other
// parts of the system to use.
func (cp *connectionPool) InitializeAddrs(addrs []string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, addr := range addrs {
		if _, ok := cp.chans[addr]; !ok {
			cp.addrs = append(cp.addrs, addr)
			cp.chans[addr] = make(chan *connection, 10)
		}
	}
}

// GetIdleConnection returns a random idle connection from the set of connections that we
// happen to have open. If no connections are available or idle, this returns nil.
func (cp *connectionPool) GetIdleConnection() *connection {
	addrs := cp.GetAllAddrs()

Address:
	for _, idx := range rndPerm(len(addrs)) {
		chn := cp.getOrCreateAddrChan(addrs[idx])

		for {
			select {
			case conn := <-chn:
				// This connection is idle (it was in channel), but let's see if it was
				// closed or not.
				if !conn.IsClosed() {
					return conn
				}
			default:
				// This will only fire when we've exhausted the channel.
				continue Address
			}
		}
	}
	return nil
}

// GetConnectionByAddr takes an address and returns a valid/open connection to this server.
// We attempt to reuse connections if we can, but if a connection is not available within
// IdleConnectionWait then we'll establish a new one.
func (cp *connectionPool) GetConnectionByAddr(addr string) (*connection, error) {
	if cp.IsClosed() {
		return nil, errors.New("connection pool is closed")
	}

	chn := cp.getOrCreateAddrChan(addr)
	for {
		select {
		case conn := <-chn:
			// Fast path: Connection is not closed, return it.
			if !conn.IsClosed() {
				return conn, nil
			}
			log.Debug("ditching closed channel", "addr", addr)

		// If the above didn't happen, we want to wait for some random period of time before
		// establishing a new connection. We randomize this to try to minimize the thundering
		// herd issue and reduce the overall connection count.
		case <-time.After(time.Duration(rndIntn(int(cp.conf.IdleConnectionWait)))):
			// No connections were active within some threshold so let's start up a new one
			// if we didn't get closed in the above wait.
			if cp.IsClosed() {
				return nil, errors.New("connection pool is closed")
			}
			conn, err := newTCPConnection(addr, cp.conf.DialTimeout)
			if err != nil {
				log.Error("cannot connect",
					"addr", addr,
					"error", err)
				return nil, err
			}

			log.Debug("made new connection",
				"addr", addr)
			return conn, nil
		}
	}
}

// Close sets the connection pool's end state, no further connections will be returned
// and any existing connections will be closed out.
func (cp *connectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.closed = true
}

// IsClosed returns whether or not this pool is closed.
func (cp *connectionPool) IsClosed() bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return cp.closed
}

// Idle takes a now idle connection and makes it available for other users. This should be
// called in a goroutine so as not to block the original caller, as this function may take
// some time to return.
func (cp *connectionPool) Idle(conn *connection) {
	// If the connection is closed, throw it away. But if the connection pool is closed, then
	// close the connection.
	if conn.IsClosed() {
		return
	} else if cp.IsClosed() {
		conn.Close()
		return
	}

	chn := cp.getOrCreateAddrChan(conn.addr)
	select {
	case chn <- conn:
		// Do nothing, connection was requeued.
	case <-time.After(cp.conf.IdleConnectionWait):
		// The queue is full for a while, discard this connection.
		log.Debug("discarding idle connection", "addr", conn.addr)
		conn.Close()
	}
}
