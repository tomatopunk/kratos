package redis

import (
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-kratos/kratos/pkg/container/pool"
	xtime "github.com/go-kratos/kratos/pkg/time"
)

func TestRedis(t *testing.T) {
	testSet(t, testRedis)
	testSend(t, testRedis)
	testGet(t, testRedis)
	testErr(t, testRedis)
	if err := testRedis.Close(); err != nil {
		t.Errorf("redis: close error(%v)", err)
	}
}

func testSet(t *testing.T, p *Redis) {
	var (
		key   = "test"
		value = "test"
		conn  = p.Conn(context.TODO())
	)
	defer conn.Close()
	if reply, err := conn.Do("set", key, value); err != nil {
		t.Errorf("redis: conn.Do(SET, %s, %s) error(%v)", key, value, err)
	} else {
		t.Logf("redis: set status: %s", reply)
	}
}

func testSend(t *testing.T, p *Redis) {
	var (
		key    = "test"
		value  = "test"
		expire = 1000
		conn   = p.Conn(context.TODO())
	)
	defer conn.Close()
	if err := conn.Send("SET", key, value); err != nil {
		t.Errorf("redis: conn.Send(SET, %s, %s) error(%v)", key, value, err)
	}
	if err := conn.Send("EXPIRE", key, expire); err != nil {
		t.Errorf("redis: conn.Send(EXPIRE key(%s) expire(%d)) error(%v)", key, expire, err)
	}
	if err := conn.Flush(); err != nil {
		t.Errorf("redis: conn.Flush error(%v)", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := conn.Receive(); err != nil {
			t.Errorf("redis: conn.Receive error(%v)", err)
			return
		}
	}
	t.Logf("redis: set value: %s", value)
}

func testGet(t *testing.T, p *Redis) {
	var (
		key  = "test"
		conn = p.Conn(context.TODO())
	)
	defer conn.Close()
	if reply, err := conn.Do("GET", key); err != nil {
		t.Errorf("redis: conn.Do(GET, %s) error(%v)", key, err)
	} else {
		t.Logf("redis: get value: %s", reply)
	}
}

func testErr(t *testing.T, p *Redis) {
	conn := p.Conn(context.TODO())
	if err := conn.Close(); err != nil {
		t.Errorf("redis: close error(%v)", err)
	}
	if err := conn.Err(); err == nil {
		t.Errorf("redis: err not nil")
	} else {
		t.Logf("redis: err: %v", err)
	}
}

func BenchmarkRedis(b *testing.B) {
	benchmarkPool, _ := Dial("tcp", testRedisAddr,
		ConnectTimeout(1*time.Second),
		ReadTimeout(1*time.Second),
		WriteTimeout(1*time.Second),
		Pool(pool.Config{
			Active:      10,
			Idle:        5,
			IdleTimeout: xtime.Duration(90 * time.Second),
		}),
	)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn := benchmarkPool.Conn(context.TODO())
			if err := conn.Close(); err != nil {
				b.Errorf("redis: close error(%v)", err)
			}
		}
	})
	if err := benchmarkPool.Close(); err != nil {
		b.Errorf("redis: close error(%v)", err)
	}
}

var testRedisCommands = []struct {
	args     []interface{}
	expected interface{}
}{
	{
		[]interface{}{"PING"},
		"PONG",
	},
	{
		[]interface{}{"SET", "foo", "bar"},
		"OK",
	},
	{
		[]interface{}{"GET", "foo"},
		[]byte("bar"),
	},
	{
		[]interface{}{"GET", "nokey"},
		nil,
	},
	{
		[]interface{}{"MGET", "nokey", "foo"},
		[]interface{}{nil, []byte("bar")},
	},
	{
		[]interface{}{"INCR", "mycounter"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "mylist", "foo"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "mylist", "bar"},
		int64(2),
	},
	{
		[]interface{}{"LRANGE", "mylist", 0, -1},
		[]interface{}{[]byte("bar"), []byte("foo")},
	},
}

func TestRedis_Do(t *testing.T) {
	r := testRedis
	r.Do(context.TODO(), "FLUSHDB")

	for _, cmd := range testRedisCommands {
		actual, err := r.Do(context.TODO(), cmd.args[0].(string), cmd.args[1:]...)
		if err != nil {
			t.Errorf("Do(%v) returned error %v", cmd.args, err)
			continue
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Do(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
	err := r.Close()
	if err != nil {
		t.Errorf("Close() error %v", err)
	}
}

//func TestRedis_Conn(t *testing.T) {
//
//	type args struct {
//		ctx context.Context
//	}
//	tests := []struct {
//		name    string
//		p       *Redis
//		args    args
//		wantErr bool
//		g       int
//		c       int
//	}{
//		{
//			"Close",
//			NewRedis(&Config{
//				Config: &pool.Config{
//					Active: 1,
//					Idle:   1,
//				},
//				Proto:        "tcp",
//				Addr:         testRedisAddr,
//				DialTimeout:  xtime.Duration(time.Second),
//				ReadTimeout:  xtime.Duration(time.Second),
//				WriteTimeout: xtime.Duration(time.Second),
//			}),
//			args{context.TODO()},
//			false,
//			3,
//			3,
//		},
//		{
//			"CloseExceededPoolSize",
//			NewRedis(&Config{
//				Config: &pool.Config{
//					Active: 1,
//					Idle:   1,
//				},
//				Proto:        "tcp",
//				Addr:         testRedisAddr,
//				DialTimeout:  xtime.Duration(time.Second),
//				ReadTimeout:  xtime.Duration(time.Second),
//				WriteTimeout: xtime.Duration(time.Second),
//			}),
//			args{context.TODO()},
//			true,
//			5,
//			3,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			for i := 1; i <= tt.g; i++ {
//				got := tt.p.Conn(tt.args.ctx)
//				if err := got.Close(); err != nil {
//					if !tt.wantErr {
//						t.Error(err)
//					}
//				}
//				if i <= tt.c {
//					if err := got.Close(); err != nil {
//						t.Error(err)
//					}
//				}
//			}
//		})
//	}
//}

func BenchmarkRedisDoPing(b *testing.B) {
	r := testRedis
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Do(context.Background(), "PING"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisDoSET(b *testing.B) {
	r := testRedis
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Do(context.Background(), "SET", "a", "b"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedisDoGET(b *testing.B) {
	r := testRedis
	defer r.Close()
	r.Do(context.Background(), "SET", "a", "b")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Do(context.Background(), "GET", "b"); err != nil {
			b.Fatal(err)
		}
	}
}

var dialErrors = []struct {
	rawurl        string
	expectedError string
}{
	{
		"localhost",
		"invalid redis URL scheme",
	},
	// The error message for invalid hosts is diffferent in different
	// versions of Go, so just check that there is an error message.
	{
		"redis://weird url",
		"",
	},
	{
		"redis://foo:bar:baz",
		"",
	},
	{
		"http://www.google.com",
		"invalid redis URL scheme: http",
	},
	{
		"redis://localhost:6379/abc123",
		"invalid database: abc123",
	},
}

func TestDialURLErrors(t *testing.T) {
	for _, d := range dialErrors {
		_, err := DialURL(d.rawurl)
		if err == nil || !strings.Contains(err.Error(), d.expectedError) {
			t.Errorf("DialURL did not return expected error (expected %v to contain %s)", err, d.expectedError)
		}
	}
}

func TestDialURLPort(t *testing.T) {
	checkPort := func(network, address string) (net.Conn, error) {
		if address != "localhost:6379" {
			t.Errorf("DialURL did not set port to 6379 by default (got %v)", address)
		}
		return nil, nil
	}
	_, err := DialURL("redis://localhost", NetDial(checkPort))
	if err != nil {
		t.Error("dial error:", err)
	}
}

/*
func TestDialURLHost(t *testing.T) {
	checkHost := func(network, address string) (net.Conn, error) {
		if address != "localhost:6379" {
			t.Errorf("DialURL did not set host to localhost by default (got %v)", address)
		}
		return nil, nil
	}
	_, err := DialURL("redis://:6379", NetDial(checkHost))
	if err != nil {
		t.Error("dial error:", err)
	}
}

func TestDialURLPassword(t *testing.T) {
	var buf bytes.Buffer
	_, err := DialURL("redis://x:abc123@localhost", dialTestConn(strings.NewReader("+OK\r\n"), &buf))
	if err != nil {
		t.Error("dial error:", err)
	}
	expected := "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n"
	actual := buf.String()
	if actual != expected {
		t.Errorf("commands = %q, want %q", actual, expected)
	}
}

func TestDialURLDatabase(t *testing.T) {
	var buf bytes.Buffer
	_, err := DialURL("redis://localhost/3", dialTestConn(strings.NewReader("+OK\r\n"), &buf))
	if err != nil {
		t.Error("dial error:", err)
	}
	expected := "*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n"
	actual := buf.String()
	if actual != expected {
		t.Errorf("commands = %q, want %q", actual, expected)
	}
}

// Connect to local instance of Redis running on the default port.
func ExampleDial() {
	c, err := Dial("tcp", ":6379")
	if err != nil {
		// handle error
	}
	defer c.Close()
}

// Connect to remote instance of Redis using a URL.
func ExampleDialURL() {
	c, err := DialURL(os.Getenv("REDIS_URL"))
	if err != nil {
		// handle connection error
	}
	defer c.Close()
}
*/
