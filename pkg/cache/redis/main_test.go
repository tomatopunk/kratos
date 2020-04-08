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
	testRedis     *Redis
)

func TestMain(m *testing.M) {
	flag.Set("f", "./test/docker-compose.yaml")
	if err := lich.Setup(); err != nil {
		panic(err)
	}
	defer lich.Teardown()

	testRedis, _ = Dial("tcp", testRedisAddr,
		ConnectTimeout(1*time.Second),
		ReadTimeout(1*time.Second),
		WriteTimeout(1*time.Second),
		Pool(pool.Config{
			Active:      20,
			Idle:        2,
			IdleTimeout: xtime.Duration(90 * time.Second),
		}),
	)

	testRedis.Do(context.TODO(), "FLUSHDB")

	ret := m.Run()
	os.Exit(ret)
}
