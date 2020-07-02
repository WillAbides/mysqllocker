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
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var _mysqlAddr string

func mysqlAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("MYSQL_ADDR")
	if addr != "" {
		return addr
	}
	out, err := exec.Command("docker-compose", "port", "mysql", "3306").Output()
	require.NoError(t, err)
	_mysqlAddr = strings.TrimSpace(string(out))
	return _mysqlAddr
}

func getDB(t *testing.T) *sql.DB {
	t.Helper()
	addr := mysqlAddr(t)
	db, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/", addr))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second)
	defer cancel()
	err = mysql.SetLogger(log.New(ioutil.Discard, "", 0))
	require.NoError(t, err)
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
		lockName := "testlock"
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, ok, err := Lock(ctx, db, lockName, time.Millisecond)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, errs)
		time.Sleep(3 * time.Millisecond)
		cancel()
		require.NoError(t, <-errs)
	})

	t.Run("can't get the same lock twice", func(t *testing.T) {
		lockName := "testlock"
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, ok, err := Lock(ctx, db, lockName, time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		errs, ok, err := Lock(ctx, db, lockName, time.Second)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, errs)
	})

	t.Run("release and relock", func(t *testing.T) {
		lockName := "testlock"
		db := getDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errs, ok, err := Lock(ctx, db, lockName, time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, errs)
		cancel()
		require.NoError(t, <-errs)
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		errs, ok, err = Lock(ctx2, db, lockName, time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, errs)
		cancel2()
		require.NoError(t, <-errs)
	})
}

func Test_checkLock(t *testing.T) {
	t.Run("no lock", func(t *testing.T) {
		ctx := context.Background()
		db := getDB(t)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		got, err := checkLock(ctx, conn, "nolocktest")
		require.NoError(t, err)
		require.False(t, got)
	})

	t.Run("conn has lock", func(t *testing.T) {
		lockName := "connhaslock"
		ctx := context.Background()
		db := getDB(t)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		ok, err := getLock(ctx, conn, lockName)
		require.NoError(t, err)
		require.True(t, ok)
		got, err := checkLock(ctx, conn, lockName)
		require.NoError(t, err)
		require.True(t, got)
	})

	t.Run("other conn has lock", func(t *testing.T) {
		lockName := "otherconnhaslock"
		ctx := context.Background()
		db := getDB(t)
		conn1, err := db.Conn(ctx)
		require.NoError(t, err)
		conn2, err := db.Conn(ctx)
		require.NoError(t, err)
		ok, err := getLock(ctx, conn1, lockName)
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = checkLock(ctx, conn2, lockName)
		require.NoError(t, err)
		require.False(t, ok)
	})
}
