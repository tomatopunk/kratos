package redis

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/go-kratos/kratos/pkg/container/pool"
	"github.com/go-kratos/kratos/pkg/testing/lich"
	xtime "github.com/go-kratos/kratos/pkg/time"
)

var (
	testRedisAddr = "localhost:6379"
)

func getTestRedis() *Redis {
	testRedis, err := Dial("tcp", testRedisAddr,
		ConnectTimeout(1*time.Second),
		ReadTimeout(1*time.Second),
		WriteTimeout(1*time.Second),
		Pool(pool.Config{
			Active:      20,
			Idle:        2,
			IdleTimeout: xtime.Duration(90 * time.Second),
		}),
	)
	if err != nil {
		panic(err)
	}
	testRedis.Do(context.TODO(), "FLUSHDB")
	return testRedis
}

func TestMain(m *testing.M) {
	flag.Set("f", "./test/docker-compose.yaml")
	if err := lich.Setup(); err != nil {
		panic(err)
	}
	defer lich.Teardown()

	ret := m.Run()
	os.Exit(ret)
}
