package kafka

import (
	"errors"
	"sync"
	"time"
)

// backend stores information about a given backend. All access to this data should be done
// through methods to ensure accurate counting and limiting.
type backend struct {
	conf    BrokerConf
	addr    string
	channel chan *connection

	// Used for storing links to all connections we ever make, this is a debugging
	// tool to try to help find leaks of connections. All access is protected by mu.
	mu        *sync.Mutex
	conns     []*connection
	counter   int
	debugTime time.Time
}

// getIdleConnection returns a connection if and only if there is an active, idle connection
// that already exists.
func (b *backend) GetIdleConnection() *connection {
	for {
		select {
		case conn := <-b.channel:
			if !conn.IsClosed() {
				return conn
			}
			b.removeConnection(conn)

		default:
			return nil
		}
	}
}

// GetConnection does a full connection logic: attempt to return an idle connection, if
// none are available then wait for up to the IdleConnectionWait time for one, else finally
// establish a new connection if we aren't at the limit. If we are, then continue waiting
// in increments of the idle time for a connection or the limit to come down before making
// a new connection. This could potentially block up to the DialTimeout.
func (b *backend) GetConnection() *connection {
	dialTimeout := time.After(b.conf.DialTimeout)
	for {
		select {
		case <-dialTimeout:
			return nil

		case conn := <-b.channel:
			if !conn.IsClosed() {
				return conn
			}
			b.removeConnection(conn)

		case <-time.After(time.Duration(rndIntn(int(b.conf.IdleConnectionWait)))):
			conn, err := b.getNewConnection()
			if err != nil {
				return nil
			} else if conn != nil {
				return conn
			}
		}
	}
}

// debugHitMaxConnections will potentially do some debugging output to help diagnose situations
// where we're hitting connection limits.
func (b *backend) debugHitMaxConnections() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if time.Now().Before(b.debugTime) {
		return
	}
	b.debugTime = time.Now().Add(10 * time.Second)

	log.Warn("DEBUG: hit max connections",
		"counter", b.counter,
		"len(conns)", len(b.conns))
	for idx, conn := range b.conns {
		log.Warn("DEBUG: connection",
			"idx", idx,
			"conn", conn,
			"closed", conn.IsClosed(),
			"age", time.Now().Sub(conn.StartTime()))
	}
}

// getNewConnection establishes a new connection if and only if we haven't hit the limit, else
// it will return nil. If an error is returned, we failed to connect to the server and should
// abort the flow. This takes a lock on the mutex which means we can only have a single new
// connection request in-flight at one time. Takes the mutex.
func (b *backend) getNewConnection() (*connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.counter >= b.conf.ConnectionLimit {
		go b.debugHitMaxConnections()
		return nil, nil
	}

	// Be careful about the situation where newTCPConnection could never return, so
	// we want to always make sure getNewConnection eventually returns. Else, we can
	// lose the connection pool.

	type connResult struct {
		conn *connection
		err  error
	}
	connChan := make(chan connResult, 1)

	go func() {
		log.Debug("making new connection", "addr", b.addr)
		if conn, err := newTCPConnection(b.addr, b.conf.DialTimeout); err != nil {
			log.Error("cannot connect", "addr", b.addr, "error", err)
			connChan <- connResult{nil, err}
		} else {
			connChan <- connResult{conn, nil}
		}
	}()

	select {
	case <-time.After(b.conf.DialTimeout):
		log.Error("DEBUG: timeout waiting for dial", "addr", b.addr)
		return nil, nil

	case result := <-connChan:
		if result.err != nil {
			return nil, result.err
		} else {
			b.counter++
			b.conns = append(b.conns, result.conn)
			return result.conn, nil
		}
	}

}

// removeConnection removes the given connection from our tracking. It also decrements the
// open connection count. This takes the mutex.
func (b *backend) removeConnection(conn *connection) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for idx, c := range b.conns {
		if c == conn {
			b.counter--
			b.conns = append(b.conns[0:idx], b.conns[idx+1:]...)
			return
		}
	}
	log.Error("unknown connection in removeConnection", "conn", conn)
}

// Idle is called when a connection should be returned to the store.
func (b *backend) Idle(conn *connection) {
	// If the connection is closed, throw it away. But if the connection pool is closed, then
	// close the connection.
	if conn.IsClosed() {
		b.removeConnection(conn)
		return
	}

	// If we're above the idle connection limit, discard the connection.
	if len(b.channel) >= b.conf.IdleConnectionLimit {
		conn.Close()
		b.removeConnection(conn)
		return
	}

	select {
	case b.channel <- conn:
		// Do nothing, connection was requeued.
	case <-time.After(b.conf.IdleConnectionWait):
		// The queue is full for a while, discard this connection.
		b.removeConnection(conn)
		conn.Close()
	}
}

// NumOpenConnections returns a counter of how may connections are open.
func (b *backend) NumOpenConnections() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.counter
}

// connectionPool is a way for us to manage multiple connections to a Kafka broker in a way
// that balances out throughput with overall number of connections.
type connectionPool struct {
	conf BrokerConf

	// mu protects the below members of this struct. This mutex must only be used by
	// connectionPool.
	mu       *sync.RWMutex
	closed   bool
	backends map[string]*backend
	addrs    []string
}

// newConnectionPool creates a connection pool and initializes it.
func newConnectionPool(conf BrokerConf) *connectionPool {
	return &connectionPool{
		conf:     conf,
		mu:       &sync.RWMutex{},
		backends: make(map[string]*backend),
		addrs:    make([]string, 0),
	}
}

// newBackend creates a new backend structure.
func (cp *connectionPool) newBackend(addr string) *backend {
	return &backend{
		mu:      &sync.Mutex{},
		conf:    cp.conf,
		addr:    addr,
		channel: make(chan *connection, cp.conf.IdleConnectionLimit),
	}
}

// getBackend fetches a channel for a given address. This takes the read lock. If no
// channel exists, nil is returned.
func (cp *connectionPool) getBackend(addr string) *backend {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		return nil
	}

	if _, ok := cp.backends[addr]; !ok {
		return nil
	}
	return cp.backends[addr]
}

// getOrCreateBackend fetches a channel for a given address and, if one doesn't exist,
// creates it. This function takes the write lock against the pool.
func (cp *connectionPool) getOrCreateBackend(addr string) *backend {
	// Fast path: only gets a read lock
	if be := cp.getBackend(addr); be != nil {
		return be
	}

	// Did not exist, take the slow path and make a new backend
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return nil
	}

	if _, ok := cp.backends[addr]; !ok {
		cp.addrs = append(cp.addrs, addr)
		cp.backends[addr] = cp.newBackend(addr)
	}
	return cp.backends[addr]
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
		if _, ok := cp.backends[addr]; !ok {
			cp.addrs = append(cp.addrs, addr)
			cp.backends[addr] = cp.newBackend(addr)
		}
	}
}

// GetIdleConnection returns a random idle connection from the set of connections that we
// happen to have open. If no connections are available or idle, this returns nil.
func (cp *connectionPool) GetIdleConnection() *connection {
	addrs := cp.GetAllAddrs()

	for _, idx := range rndPerm(len(addrs)) {
		if be := cp.getOrCreateBackend(addrs[idx]); be != nil {
			if conn := be.GetIdleConnection(); conn != nil {
				return conn
			}
		}
	}
	return nil
}

// GetConnectionByAddr takes an address and returns a valid/open connection to this server.
// We attempt to reuse connections if we can, but if a connection is not available within
// IdleConnectionWait then we'll establish a new one. This can block a long time.
func (cp *connectionPool) GetConnectionByAddr(addr string) (*connection, error) {
	if cp.IsClosed() {
		return nil, errors.New("connection pool is closed")
	}

	if be := cp.getOrCreateBackend(addr); be != nil {
		if conn := be.GetConnection(); conn != nil {
			return conn, nil
		}
	}
	return nil, errors.New("failed to get connection")
}

// Close sets the connection pool's end state, no further connections will be returned
// and any existing connections will be closed out.
func (cp *connectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, backend := range cp.backends {
	Loop:
		for {
			select {
			case conn := <-backend.channel:
				defer conn.Close()
			default:
				break Loop
			}
		}
	}

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
	if cp.IsClosed() {
		conn.Close()
		return
	}

	if be := cp.getOrCreateBackend(conn.addr); be != nil {
		be.Idle(conn)
	} else {
		conn.Close()
	}
}
