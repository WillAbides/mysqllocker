package mysqllocker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

// Lock gets a named lock from mysql using GET_LOCK() and holds it until ctx is canceled.
// Returns an error channel that closes when the lock is released by ctx closed or reports an error if the lock cannot be
// renewed. Named locks in mysql are good until either they are explicitly released or the session ends. That is why this
// method creates a goroutine that continually renews the lock pausing relockInterval between. That prevents the session
// from being closed for inactivity.
// getLockTimeout is the duration to wait for a lock before giving up.
func Lock(ctx context.Context, db *sql.DB, lockName string, relockInterval, getLockTimeout time.Duration) (<-chan error, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	ok, err := getLock(ctx, conn, lockName, getLockTimeout)
	if err != nil || !ok {
		_ = conn.Close() //nolint:errcheck
		err = fmt.Errorf("could not obtain lock")
		return nil, err
	}

	errs := make(chan error)

	// launch goroutine to hold the lock on this connection
	go holdLock(ctx, conn, lockName, relockInterval, errs)

	return errs, nil
}

// holdLock maintains an existing lock on a conn until ctx is canceled or there is an error by periodically checking that
// the lock holder's id is the same as the current connection_id
func holdLock(ctx context.Context, conn *sql.Conn, lockName string, relockInterval time.Duration, errs chan error) {
	ticker := time.NewTicker(relockInterval)

	// keep checking that this connection still has the lock every 10s until it doesn't
	var err error
	for haveLock := true; haveLock; {
		select {
		case <-ctx.Done():
			haveLock = false
		case <-ticker.C:
			haveLock, err = checkLock(ctx, conn, lockName)
			// If we got an error and the context is closed, we discard the error and break the loop by setting haveLock = false
			if err != nil {
				haveLock = false
				if ctx.Err() == nil {
					errs <- err
				}
			}
		}
	}

	err = releaseLock(conn, lockName)
	if err != nil {
		errs <- err
	}
	err = conn.Close()
	if err != nil && err != sql.ErrConnDone {
		errs <- err
	}
	close(errs)
	ticker.Stop()
}

// releaseLock releases the lock named lockName from the given connection
func releaseLock(conn *sql.Conn, lockName string) error { //nolint:interfacer
	// use our own context so we can attempt to release a lock even after the calling function's context has been closed
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, `DO RELEASE_LOCK(?)`, lockName)
	// if the connection is already closed, then the lock is already released and we shouldn't return an error
	if err == driver.ErrBadConn {
		err = nil
	}
	return err
}

// getLock attempts GET_LOCK on the given conn.  Does not attempt to hold the lock.
func getLock(ctx context.Context, conn *sql.Conn, lockName string, timeout time.Duration) (bool, error) {
	waitSeconds := 0
	var cancel context.CancelFunc
	if timeout > 0 {
		waitSeconds = -1
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var gotLock sql.NullBool
	row := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, ?)`, lockName, waitSeconds)
	err := row.Scan(&gotLock)
	// needs to be both Valid and true to return true
	return gotLock.Valid && gotLock.Bool, err
}

// checkLock checks whether the given conn holds a named lock. Returns true if it does.
func checkLock(ctx context.Context, conn *sql.Conn, lockName string) (bool, error) {
	var gotLock sql.NullBool
	row := conn.QueryRowContext(ctx, `SELECT(IS_USED_LOCK(?) = CONNECTION_ID())`, lockName)
	err := row.Scan(&gotLock)
	// needs to be both Valid and true to return true
	return gotLock.Valid && gotLock.Bool, err
}
