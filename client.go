// Package vtvalkey is a lightweight Valkey/Redis client for embedded systems.
// It implements only the commands needed by Vector Technology apps, avoiding
// the ~4 MB binary overhead of full-featured client libraries.
package vtvalkey

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// ErrNil is returned when a key does not exist.
var ErrNil = errors.New("vtvalkey: nil")

// Options configures the client connection.
type Options struct {
	Address      string // host:port (default "127.0.0.1:6379")
	Password     string // AUTH password (empty = skip)
	DB           int    // SELECT database (0 = default)
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (o *Options) address() string {
	if o.Address != "" {
		return o.Address
	}
	return "127.0.0.1:6379"
}

func (o *Options) dialTimeout() time.Duration {
	if o.DialTimeout > 0 {
		return o.DialTimeout
	}
	return 5 * time.Second
}

// Client is a Valkey/Redis client. All methods are safe for concurrent use.
type Client struct {
	opts Options
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

// New creates a new client and connects to Valkey.
func New(opts Options) (*Client, error) {
	c := &Client{opts: opts}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.opts.address(), c.opts.dialTimeout())
	if err != nil {
		return fmt.Errorf("vtvalkey: dial %s: %w", c.opts.address(), err)
	}
	c.conn = conn
	c.r = bufio.NewReaderSize(conn, 64*1024)

	if c.opts.Password != "" {
		if err := c.doSimple("AUTH", c.opts.Password); err != nil {
			conn.Close()
			return fmt.Errorf("vtvalkey: auth: %w", err)
		}
	}
	if c.opts.DB != 0 {
		if err := c.doSimple("SELECT", strconv.Itoa(c.opts.DB)); err != nil {
			conn.Close()
			return fmt.Errorf("vtvalkey: select db %d: %w", c.opts.DB, err)
		}
	}
	return nil
}

func (c *Client) doSimple(args ...string) error {
	if err := c.setWriteDeadline(); err != nil {
		return err
	}
	if err := writeCommand(c.conn, args...); err != nil {
		return err
	}
	if err := c.setReadDeadline(); err != nil {
		return err
	}
	rp, err := readReply(c.r)
	if err != nil {
		return err
	}
	return rp.toError()
}

func (c *Client) setWriteDeadline() error {
	if c.opts.WriteTimeout > 0 {
		return c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	}
	return c.conn.SetWriteDeadline(time.Time{})
}

func (c *Client) setReadDeadline() error {
	if c.opts.ReadTimeout > 0 {
		return c.conn.SetReadDeadline(time.Now().Add(c.opts.ReadTimeout))
	}
	return c.conn.SetReadDeadline(time.Time{})
}

// reconnect drops the current connection and establishes a new one.
func (c *Client) reconnect() error {
	if c.conn != nil {
		c.conn.Close()
	}
	return c.connect()
}

// exec sends a single command and returns the reply. Caller must hold c.mu.
func (c *Client) exec(args ...string) (reply, error) {
	rp, err := c.execOnce(args...)
	if err != nil {
		// One reconnect attempt on network error
		if reconnErr := c.reconnect(); reconnErr != nil {
			return reply{}, fmt.Errorf("vtvalkey: reconnect failed: %w (original: %v)", reconnErr, err)
		}
		return c.execOnce(args...)
	}
	return rp, nil
}

func (c *Client) execOnce(args ...string) (reply, error) {
	if err := c.setWriteDeadline(); err != nil {
		return reply{}, err
	}
	if err := writeCommand(c.conn, args...); err != nil {
		return reply{}, err
	}
	if err := c.setReadDeadline(); err != nil {
		return reply{}, err
	}
	return readReply(c.r)
}

// Close closes the connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// --- Commands ---------------------------------------------------------------

// Get returns the string value of key.
func (c *Client) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("GET", key)
	if err != nil {
		return "", err
	}
	return rp.toString()
}

// Set sets key to value. If ttl > 0, sets expiration.
func (c *Client) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var rp reply
	var err error
	if ttl > 0 {
		secs := strconv.FormatInt(int64(ttl.Seconds()), 10)
		rp, err = c.exec("SET", key, value, "EX", secs)
	} else {
		rp, err = c.exec("SET", key, value)
	}
	if err != nil {
		return err
	}
	return rp.toError()
}

// SetNX sets key to value only if it does not exist, with the given TTL.
// Returns (true, nil) on win, (false, nil) when the key already existed.
func (c *Client) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var args []string
	if ttl > 0 {
		args = []string{"SET", key, value, "EX", strconv.FormatInt(int64(ttl.Seconds()), 10), "NX"}
	} else {
		args = []string{"SET", key, value, "NX"}
	}
	rp, err := c.exec(args...)
	if err != nil {
		return false, err
	}
	// SET ... NX returns nil bulk when the key already existed.
	if rp.isNil {
		return false, nil
	}
	if err := rp.toError(); err != nil {
		return false, err
	}
	return true, nil
}

// Del deletes one or more keys.
func (c *Client) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := make([]string, 0, 1+len(keys))
	args = append(args, "DEL")
	args = append(args, keys...)
	rp, err := c.exec(args...)
	if err != nil {
		return err
	}
	return rp.toError()
}

// IncrBy increments key by delta and returns the new value.
func (c *Client) IncrBy(_ context.Context, key string, delta int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("INCRBY", key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	return rp.toInt64()
}

// Expire sets a TTL on key in seconds.
func (c *Client) Expire(_ context.Context, key string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	secs := strconv.FormatInt(int64(ttl.Seconds()), 10)
	rp, err := c.exec("EXPIRE", key, secs)
	if err != nil {
		return err
	}
	return rp.toError()
}

// HSet sets fields on a hash key. fields is alternating key, value pairs.
func (c *Client) HSet(_ context.Context, key string, fields ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := make([]string, 0, 2+len(fields))
	args = append(args, "HSET", key)
	args = append(args, fields...)
	rp, err := c.exec(args...)
	if err != nil {
		return err
	}
	return rp.toError()
}

// HDel removes one or more fields from a hash key. Missing fields are
// silently ignored; deleting the last field also deletes the key (Valkey
// semantics). Returns the count of fields actually removed.
func (c *Client) HDel(_ context.Context, key string, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	args := make([]string, 0, 2+len(fields))
	args = append(args, "HDEL", key)
	args = append(args, fields...)
	rp, err := c.exec(args...)
	if err != nil {
		return 0, err
	}
	if err := rp.toError(); err != nil {
		return 0, err
	}
	return rp.num, nil
}

// HSetWithTTL sets fields on a hash key and applies a TTL in one step.
// The HSet error is returned; Expire failure is logged but not returned.
func (c *Client) HSetWithTTL(ctx context.Context, key string, ttl time.Duration, fields ...string) error {
	if err := c.HSet(ctx, key, fields...); err != nil {
		return err
	}
	_ = c.Expire(ctx, key, ttl)
	return nil
}

// HGetAll returns all fields and values of a hash as a map.
func (c *Client) HGetAll(_ context.Context, key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("HGETALL", key)
	if err != nil {
		return nil, err
	}
	return rp.toStrMap()
}

// SAdd adds members to a set.
func (c *Client) SAdd(_ context.Context, key string, members ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := make([]string, 0, 2+len(members))
	args = append(args, "SADD", key)
	args = append(args, members...)
	rp, err := c.exec(args...)
	if err != nil {
		return err
	}
	return rp.toError()
}

// SRem removes members from a set.
func (c *Client) SRem(_ context.Context, key string, members ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := make([]string, 0, 2+len(members))
	args = append(args, "SREM", key)
	args = append(args, members...)
	rp, err := c.exec(args...)
	if err != nil {
		return err
	}
	return rp.toError()
}

// SMembers returns all members of a set.
func (c *Client) SMembers(_ context.Context, key string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	return rp.toStrSlice()
}

// SCard returns the number of members in a set.
func (c *Client) SCard(_ context.Context, key string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("SCARD", key)
	if err != nil {
		return 0, err
	}
	return rp.toInt64()
}

// LPush prepends one or more values to a list.
func (c *Client) LPush(_ context.Context, key string, values ...string) error {
	args := make([]string, 0, 2+len(values))
	args = append(args, "LPUSH", key)
	args = append(args, values...)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doSimple(args...)
}

// RPush appends one or more values to a list.
func (c *Client) RPush(_ context.Context, key string, values ...string) error {
	args := make([]string, 0, 2+len(values))
	args = append(args, "RPUSH", key)
	args = append(args, values...)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doSimple(args...)
}

// LRange returns the specified range of elements from the list at key.
// start and stop are zero-based; -1 = last element.
func (c *Client) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("LRANGE", key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10))
	if err != nil {
		return nil, err
	}
	if err := rp.toError(); err != nil {
		return nil, err
	}
	return rp.toStrSlice()
}

// LTrim trims the list at key so only elements in [start, stop] remain.
func (c *Client) LTrim(_ context.Context, key string, start, stop int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("LTRIM", key,
		strconv.FormatInt(start, 10),
		strconv.FormatInt(stop, 10))
	if err != nil {
		return err
	}
	return rp.toError()
}

// LPop removes and returns the first element of a list.
// Returns ("", nil) if the list is empty or does not exist.
func (c *Client) LPop(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rp, err := c.exec("LPOP", key)
	if err != nil {
		return "", err
	}
	if rp.isNil {
		return "", nil
	}
	return rp.toString()
}

// ScanEntry holds the result of a SCAN iteration.
type ScanEntry struct {
	Cursor   uint64
	Elements []string
}

// Scan iterates keys matching pattern. Pass cursor=0 to start.
func (c *Client) Scan(_ context.Context, cursor uint64, match string, count int) (ScanEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	args := []string{"SCAN", strconv.FormatUint(cursor, 10)}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	if count > 0 {
		args = append(args, "COUNT", strconv.Itoa(count))
	}

	rp, err := c.exec(args...)
	if err != nil {
		return ScanEntry{}, err
	}
	if err := rp.toError(); err != nil {
		return ScanEntry{}, err
	}
	if rp.kind != replyArray || len(rp.items) != 2 {
		return ScanEntry{}, fmt.Errorf("vtvalkey: unexpected scan reply")
	}

	newCursor, err := strconv.ParseUint(rp.items[0].str, 10, 64)
	if err != nil {
		return ScanEntry{}, fmt.Errorf("vtvalkey: bad scan cursor: %w", err)
	}
	elements, err := rp.items[1].toStrSlice()
	if err != nil {
		return ScanEntry{}, err
	}
	return ScanEntry{Cursor: newCursor, Elements: elements}, nil
}

// --- Pipeline (DoMulti) -----------------------------------------------------

// Command represents a pre-built command for pipeline execution.
type Command struct {
	args []string
}

// Cmd creates a command for use with DoMulti.
func Cmd(args ...string) Command {
	return Command{args: args}
}

// Result holds a single pipeline response.
type Result struct {
	rp reply
}

// Error returns the error from this result, if any.
func (r Result) Error() error { return r.rp.toError() }

// ToString returns the string value.
func (r Result) ToString() (string, error) { return r.rp.toString() }

// ToInt64 returns the integer value.
func (r Result) ToInt64() (int64, error) { return r.rp.toInt64() }

// ToStrSlice returns a string slice.
func (r Result) ToStrSlice() ([]string, error) { return r.rp.toStrSlice() }

// ToStrMap returns a string map (for HGETALL).
func (r Result) ToStrMap() (map[string]string, error) { return r.rp.toStrMap() }

// DoMulti sends multiple commands in a pipeline and returns all results.
func (c *Client) DoMulti(_ context.Context, cmds ...Command) ([]Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	results, err := c.doMultiOnce(cmds)
	if err != nil {
		if reconnErr := c.reconnect(); reconnErr != nil {
			return nil, fmt.Errorf("vtvalkey: reconnect failed: %w (original: %v)", reconnErr, err)
		}
		return c.doMultiOnce(cmds)
	}
	return results, nil
}

func (c *Client) doMultiOnce(cmds []Command) ([]Result, error) {
	if err := c.setWriteDeadline(); err != nil {
		return nil, err
	}
	for _, cmd := range cmds {
		if err := writeCommand(c.conn, cmd.args...); err != nil {
			return nil, err
		}
	}
	if err := c.setReadDeadline(); err != nil {
		return nil, err
	}
	results := make([]Result, len(cmds))
	for i := range cmds {
		rp, err := readReply(c.r)
		if err != nil {
			return nil, err
		}
		results[i] = Result{rp: rp}
	}
	return results, nil
}
