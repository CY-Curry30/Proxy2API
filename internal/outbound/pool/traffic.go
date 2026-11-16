package pool

import (
	"net"
	"sync"

	"Proxy2API/internal/state"
)

type trackedConn struct {
	net.Conn
	once    sync.Once
	release func()
	traffic *state.UsageCounter
	ioMu    sync.RWMutex
}

func (c *trackedConn) Read(buffer []byte) (int, error) {
	c.ioMu.RLock()
	defer c.ioMu.RUnlock()
	n, err := c.Conn.Read(buffer)
	c.traffic.Add(0, int64(n))
	return n, err
}

func (c *trackedConn) Write(buffer []byte) (int, error) {
	c.ioMu.RLock()
	defer c.ioMu.RUnlock()
	n, err := c.Conn.Write(buffer)
	c.traffic.Add(int64(n), 0)
	return n, err
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	// Closing unblocks pending I/O. Wait for its final byte counts before the
	// runtime flushes the state store during shutdown.
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	c.once.Do(c.release)
	return err
}

type trackedPacketConn struct {
	net.PacketConn
	once    sync.Once
	release func()
	traffic *state.UsageCounter
	ioMu    sync.RWMutex
}

func (c *trackedPacketConn) ReadFrom(buffer []byte) (int, net.Addr, error) {
	c.ioMu.RLock()
	defer c.ioMu.RUnlock()
	n, addr, err := c.PacketConn.ReadFrom(buffer)
	c.traffic.Add(0, int64(n))
	return n, addr, err
}

func (c *trackedPacketConn) WriteTo(buffer []byte, addr net.Addr) (int, error) {
	c.ioMu.RLock()
	defer c.ioMu.RUnlock()
	n, err := c.PacketConn.WriteTo(buffer, addr)
	c.traffic.Add(int64(n), 0)
	return n, err
}

func (c *trackedPacketConn) Close() error {
	err := c.PacketConn.Close()
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	c.once.Do(c.release)
	return err
}
