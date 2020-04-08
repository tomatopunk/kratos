package redis

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-kratos/kratos/pkg/container/pool"
	xtime "github.com/go-kratos/kratos/pkg/time"
)

func getTestRedis() (*Redis, error) {
	return Dial("tcp", testRedisAddr,
		ReadTimeout(1*time.Second),
		WriteTimeout(1*time.Second),
		ConnectTimeout(1*time.Second),
		Pool(pool.Config{Active: 10, Idle: 2, IdleTimeout: xtime.Duration(90 * time.Second)}),
	)
}

func TestRedis_Pipeline(t *testing.T) {
	r, _ := getTestRedis()
	r.Do(context.TODO(), "FLUSHDB")

	p := r.Pipeline()

	for _, cmd := range testCommands {
		p.Send(cmd.args[0].(string), cmd.args[1:]...)
	}

	replies, err := p.Exec(context.TODO())

	i := 0
	for replies.Next() {
		cmd := testCommands[i]
		actual, err := replies.Scan()
		if err != nil {
			t.Fatalf("Receive(%v) returned error %v", cmd.args, err)
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Receive(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
		i++
	}
	err = r.Close()
	if err != nil {
		t.Errorf("Close() error %v", err)
	}
}

func ExamplePipeliner() {
	r, _ := Dial("tcp", "127.0.0.1:6349")
	defer r.Close()

	pip := r.Pipeline()
	pip.Send("SET", "hello", "world")
	pip.Send("GET", "hello")
	replies, err := pip.Exec(context.TODO())
	if err != nil {
		fmt.Printf("%#v\n", err)
	}
	for replies.Next() {
		s, err := String(replies.Scan())
		if err != nil {
			fmt.Printf("err %#v\n", err)
		}
		fmt.Printf("%#v\n", s)
	}
	// Output:
	// "OK"
	// "world"
}

func BenchmarkRedisPipelineExec(b *testing.B) {
	r, _ := getTestRedis()
	defer r.Close()

	r.Do(context.TODO(), "SET", "abcde", "fghiasdfasdf")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := r.Pipeline()
		p.Send("GET", "abcde")
		_, err := p.Exec(context.TODO())
		if err != nil {
			b.Fatal(err)
		}
	}
}
