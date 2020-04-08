// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/pkg/container/pool"
	"github.com/go-kratos/kratos/pkg/net/trace"

	"github.com/pkg/errors"
)

// Option specifies an option for dialing a Redis server.
type Option struct {
	apply func(*options)
}

type options struct {
	pool         pool.Config
	readTimeout  time.Duration
	writeTimeout time.Duration
	dial         func(network, addr string) (net.Conn, error)
	db           int
	password     string
}

// Pool specifies the pool for connection pool to the Redis server.
func Pool(p pool.Config) Option {
	return Option{func(o *options) {
		o.pool = p
	}}
}

// ReadTimeout specifies the timeout for reading a single command reply.
func ReadTimeout(d time.Duration) Option {
	return Option{func(o *options) {
		o.readTimeout = d
	}}
}

// WriteTimeout specifies the timeout for writing a single command.
func WriteTimeout(d time.Duration) Option {
	return Option{func(o *options) {
		o.writeTimeout = d
	}}
}

// ConnectTimeout specifies the timeout for connecting to the Redis server.
func ConnectTimeout(d time.Duration) Option {
	return Option{func(o *options) {
		dialer := net.Dialer{Timeout: d}
		o.dial = dialer.Dial
	}}
}

// NetDial specifies a custom dial function for creating TCP
// connections. If this option is left out, then net.Dial is
// used. DialNetDial overrides DialConnectTimeout.
func NetDial(dial func(network, addr string) (net.Conn, error)) Option {
	return Option{func(o *options) {
		o.dial = dial
	}}
}

// Database specifies the database to select when dialing a connection.
func Database(db int) Option {
	return Option{func(o *options) {
		o.db = db
	}}
}

// Password specifies the password to use when connecting to
// the Redis server.
func Password(password string) Option {
	return Option{func(o *options) {
		o.password = password
	}}
}

// Redis redis struct.
type Redis struct {
	p *pool.Slice
}

// Dial connects to the Redis server at the given network and
// address using the specified options.
func Dial(network, address string, opts ...Option) (*Redis, error) {
	o := options{
		dial: net.Dial,
	}
	for _, opt := range opts {
		opt.apply(&o)
	}

	p := pool.NewSlice(&o.pool)
	p.New = func(ctx context.Context) (io.Closer, error) {
		netConn, err := o.dial(network, address)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		c := &conn{
			conn:         netConn,
			bw:           bufio.NewWriter(netConn),
			br:           bufio.NewReader(netConn),
			readTimeout:  o.readTimeout,
			writeTimeout: o.writeTimeout,
		}

		if o.password != "" {
			if _, err := c.Do("AUTH", o.password); err != nil {
				netConn.Close()
				return nil, errors.WithStack(err)
			}
		}

		if o.db != 0 {
			if _, err := c.Do("SELECT", o.db); err != nil {
				netConn.Close()
				return nil, errors.WithStack(err)
			}
		}
		return &traceConn{
			Conn:     c,
			connTags: []trace.Tag{trace.TagString(trace.TagPeerAddress, address)},
			// FIXME(maojian): use net/trace slowlog
			slowLogThreshold: time.Duration(250 * time.Millisecond),
		}, nil
	}

	return &Redis{p: p}, nil
}

var _pathDBRegexp = regexp.MustCompile(`/(\d+)\z`)

// DialURL connects to a Redis server at the given URL using the Redis
// URI scheme. URLs should follow the draft IANA specification for the
// scheme (https://www.iana.org/assignments/uri-schemes/prov/redis).
func DialURL(rawurl string, opts ...Option) (*Redis, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if u.Scheme != "redis" {
		return nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
	}

	// As per the IANA draft spec, the host defaults to localhost and
	// the port defaults to 6379.
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// assume port is missing
		host = u.Host
		port = "6379"
	}
	if host == "" {
		host = "localhost"
	}
	address := net.JoinHostPort(host, port)

	if u.User != nil {
		password, isSet := u.User.Password()
		if isSet {
			opts = append(opts, Password(password))
		}
	}

	match := _pathDBRegexp.FindStringSubmatch(u.Path)
	if len(match) == 2 {
		db, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, errors.Errorf("invalid database: %s", u.Path[1:])
		}
		if db != 0 {
			opts = append(opts, Database(db))
		}
	} else if u.Path != "" {
		return nil, errors.Errorf("invalid database: %s", u.Path[1:])
	}

	return Dial("tcp", address, opts...)
}

func (r *Redis) getConn(ctx context.Context) Conn {
	c, err := r.p.Get(ctx)
	if err != nil {
		return errorConnection{err}
	}
	c1, _ := c.(Conn)
	return &pooledConnection{p: r.p, c: c1.WithContext(ctx), rc: c1, now: time.Now()}
}

// Do gets a new conn from pool, then execute Do with this conn, finally close this conn.
// ATTENTION: Don't use this method with transaction command like MULTI etc.
// Because every Do will close conn automatically, use r.Conn to get a raw conn for this situation.
func (r *Redis) Do(ctx context.Context, commandName string, args ...interface{}) (interface{}, error) {
	c := r.getConn(ctx)
	defer c.Close()
	return c.Do(commandName, args...)
}

// Close closes the redis.
func (r *Redis) Close() error {
	return r.p.Close()
}

// Conn returns a single connection by either opening a new connection or
// returning an existing connection from the connection pool.
// Conn will block until either a connection is returned or ctx is canceled.
// Commands run on the same Conn will be run in the same redis session.
// Every Conn must be returned to the redis pool after use by calling Conn.Close.
func (r *Redis) Conn(ctx context.Context) Conn {
	return r.getConn(ctx)
}

// Pipeline get a pipeline.
func (r *Redis) Pipeline() Pipeliner {
	return &pipeliner{r: r}
}
