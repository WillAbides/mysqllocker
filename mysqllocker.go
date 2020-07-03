package mysqllocker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

const defaultPingInterval = 10 * time.Second

type lockOpts struct {
	timeout      time.Duration
	pingInterval time.Duration
}

// LockOption is an optional value for Lock
type LockOption func(*lockOpts)

// WithTimeout sets a timeout for Lock to wait before giving up on getting a lock.
// When unset, Lock will error out immediately if the lock is unavailable.
func WithTimeout(timeout time.Duration) LockOption {
	return func(o *lockOpts) {
		o.timeout = timeout
	}
}

// WithPingInterval sets the interval for Lock to ping the connection. Default is 10 seconds.
func WithPingInterval(pingInterval time.Duration) LockOption {
	return func(o *lockOpts) {
		o.pingInterval = pingInterval
	}
}

// Lock gets a named lock from mysql using GET_LOCK() and holds it until ctx is canceled.
// It pings the db connection at a regular interval to keep it from timing out.
// If the lock is unavailable and "WithTimeout" is set, it will continue trying until it either times out or obtains a lock.
// Returns an error channel that will receive an error when the lock is released.
func Lock(ctx context.Context, db *sql.DB, lockName string, options ...LockOption) (<-chan error, error) {
	opts := &lockOpts{
		pingInterval: defaultPingInterval,
	}
	for _, o := range options {
		o(opts)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	ok, err := getLock(ctx, conn, lockName, opts.timeout)
	if err != nil || !ok {
		_ = conn.Close() //nolint:errcheck
		err = fmt.Errorf("could not obtain lock: %v", err)
		return nil, err
	}
	errs := make(chan error, 1)

	go func() {
		defer close(errs)
		ticker := time.NewTicker(opts.pingInterval)
		defer ticker.Stop()
		var lErr error
		for lErr == nil {
			select {
			case <-ctx.Done():
				lErr = ctx.Err()
			case <-ticker.C:
				lErr = conn.PingContext(ctx)
			}
		}
		releaseErr := ignoreErr(releaseLock(conn, lockName))
		if releaseErr != nil {
			lErr = releaseErr
		}
		errs <- ignoreErr(lErr)
	}()

	return errs, nil
}

var ignoreableErrs = []error{
	context.DeadlineExceeded,
	context.Canceled,
	sql.ErrConnDone,
}

// ignoreErr returns err unless it is one of ignoreableErrs
func ignoreErr(err error) error {
	for _, ignoreMe := range ignoreableErrs {
		if err == ignoreMe {
			return nil
		}
	}
	return err
}

// releaseLock releases the lock named lockName from the given connection
func releaseLock(conn *sql.Conn, lockName string) error {
	// use our own context so we can attempt to release a lock even after the calling function's context has been closed
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, `DO RELEASE_LOCK(?)`, lockName)
	// if the connection is already closed, then the lock is already released and we shouldn't return an error
	if err == driver.ErrBadConn {
		err = nil
	}
	closeErr := conn.Close()
	if err == nil {
		err = closeErr
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
