package mysqllocker

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var (
	_mysqlAddr string
	setupOnce  sync.Once
)

func mysqlAddr(t *testing.T) string {
	t.Helper()
	setupOnce.Do(func() {
		_mysqlAddr = os.Getenv("MYSQL_ADDR")
		if _mysqlAddr != "" {
			return
		}
		out, err := exec.Command("docker-compose", "port", "mysql", "3306").Output()
		require.NoError(t, err)
		_mysqlAddr = strings.TrimSpace(string(out))
		require.NoError(t, mysql.SetLogger(log.New(ioutil.Discard, "", 0)))
	})
	return _mysqlAddr
}

func getDB(t *testing.T) *sql.DB {
	t.Helper()
	addr := mysqlAddr(t)
	db, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/", addr))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err, "timed out waiting for connection")
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func TestLock(t *testing.T) {
	t.Run("locks", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, err := Lock(ctx, db, lockName, WithPingInterval(10*time.Millisecond))
		require.NoError(t, err)
		require.NotNil(t, errs)
		time.Sleep(50 * time.Millisecond)
		cancel()
		require.NoError(t, <-errs)
	})

	t.Run("can't get the same lock twice", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := Lock(ctx, db, lockName)
		require.NoError(t, err)
		errs, err := Lock(ctx, db, lockName)
		require.Error(t, err)
		require.Nil(t, errs)
	})

	t.Run("waits for lock", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx1, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel1()
		errs1, err := Lock(ctx1, db, lockName)
		require.NoError(t, err)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			require.NoError(t, <-errs1)
			wg.Done()
		}()
		ctx2, cancel2 := context.WithCancel(context.Background())
		errs2, err := Lock(ctx2, db, lockName, WithTimeout(time.Second))
		require.NoError(t, err)
		cancel2()
		require.NoError(t, <-errs2)
		wg.Wait()
	})

	t.Run("times out waiting for lock", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := Lock(ctx, db, lockName)
		require.NoError(t, err)
		timeout := time.Millisecond * 30
		startTime := time.Now()
		errs, err := Lock(ctx, db, lockName, WithTimeout(timeout))
		delta := time.Since(startTime)
		require.Error(t, err)
		require.Nil(t, errs)
		require.Greater(t, int64(delta), int64(timeout))
	})

	t.Run("release and relock", func(t *testing.T) {
		t.Parallel()
		lockName := t.Name()
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, err := Lock(ctx, db, lockName)
		require.NoError(t, err)
		require.NotNil(t, errs)
		cancel()
		require.NoError(t, <-errs)
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		errs, err = Lock(ctx2, db, lockName)
		require.NoError(t, err)
		require.NotNil(t, errs)
		cancel2()
		require.NoError(t, <-errs)
	})
}
