package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"

	"github.com/go-sql-driver/mysql"
)

var (
	// ErrInvalidCountEndpoint is thrown when there are
	// multiple endpoints or no enough endpoint specified for MySQL.
	ErrInvalidCountEndpoint = errors.New("mysql only support one endpoint")

	// ErrPutMissing is thrown when put failed.
	ErrPutMissing = errors.New("mysql put missing")

	// DefaultWatchWaitTime is how long we block for at a
	// time to check if the watched key has changed. This
	// affects the minimum time it takes to cancel a watch.
	// You can modify this at the init stage.
	DefaultWatchWaitTime = time.Second

	// ErrAbortTryLock is thrown when a user stops trying to seek the lock
	// by sending a signal to the stop chan, this is used to verify if the
	// operation succeeded
	ErrAbortTryLock = errors.New("lock operation aborted")

	// ErrLockLost is thrown when the lock has been lost
	ErrLockLost = errors.New("lock lost")

	// MaxFields represents the count of key fields does the implementant supported.
	// 'a' has 1 field, 'a/b' has 2 fields, 'a/b/c' has 3 fields, and so on. Each field
	// has 127 bytes at mostly.
	// When you want to expend the max fields, you can change the mysql table and change
	// this constants, and no more code need to be changed.
	// Caveat: It's use UNIQUE KEY indexing, but the innodb default max index key length
	// is 3072. Hence, if you want more fields, you should reduce each field's length.
	MaxFields = 8

	// rnd is used to generate session string
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
)

const (
	defaultTimeout   = time.Second * 10
	defaultLockTTL   = time.Second * 15
	defaultTTLPeriod = 3
)

// MySQL is a implementant of store.Store. At first, it's need to create the mysql
// database and table manually. There is a model "table.sql" in this package. The
// database's name and table's name is modifiable.
// You can use store.Config.Database, store.Config.Table, store.Config.Username to indicate,
// and the store.Config.Password is optional. Secondly, there only support 10 fields
// for the stored key(MaxFields), and it's scalability.
type MySQL struct {
	db      *sql.DB
	table   string
	timeout time.Duration
}

// Register registers mysql to libkv
func Register() {
	libkv.AddStore(store.MYSQL, New)
}

// New creates a new MySQL client given a list of endpoints
func New(endpoints []string, opts *store.Config) (store.Store, error) {
	if len(endpoints) != 1 {
		return nil, ErrInvalidCountEndpoint
	}

	var passward string
	if opts.Password != "" {
		passward = ":" + opts.Password
	}

	timeout := defaultTimeout
	if opts.ConnectionTimeout != timeout {
		timeout = opts.ConnectionTimeout
	}

	db, err := sql.Open("mysql",
		fmt.Sprintf("%s%s@tcp(%s)/%s?charset=utf8&interpolateParams=true&parseTime=True&loc=Local&timeout=%s&readTimeout=%s&writeTimeout=%s",
			opts.Username, passward, endpoints[0], opts.Database, timeout, timeout, timeout))
	if err != nil {
		return nil, err
	}

	return &MySQL{
		db:      db,
		table:   opts.Table,
		timeout: timeout,
	}, nil
}

func normalize(key string) string {
	return strings.TrimPrefix(path.Clean(key), "/")
}

func whereCond(key string, exact bool) string {
	fields := strings.SplitN(normalize(key), "/", MaxFields)
	tokens := make([]string, 0, MaxFields)
	for i := range fields {
		tokens = append(tokens, fmt.Sprintf("`field%d`=?", i))
	}
	if l := len(tokens); exact && l < MaxFields {
		for i := l; i < MaxFields; i++ {
			tokens = append(tokens, fmt.Sprintf("`field%d`=?", i))
		}
	}
	return strings.Join(tokens, " AND ")
}

func eachFields() string {
	fields := make([]string, 0, MaxFields)
	for i := 0; i < MaxFields; i++ {
		fields = append(fields, fmt.Sprintf("`field%d`", i))
	}
	return strings.Join(fields, ", ")
}

func splitFields(key string, exact bool) []string {
	tokens := make([]string, 0, MaxFields)
	fields := strings.SplitN(normalize(key), "/", MaxFields)
	tokens = append(tokens, fields...)
	if l := len(fields); exact && l < MaxFields {
		for i := l; i < MaxFields; i++ {
			tokens = append(tokens, "")
		}
	}
	return tokens
}

// session generate a 64 bytes string to represent current session.
func session() string {
	hn, _ := os.Hostname()
	if l := len(hn); l > 30 {
		hn = hn[l-30:]
	}
	return fmt.Sprintf("%s@%016X@%016X", hn, time.Now().UnixNano(), rnd.Int63())
}

// Close the MySQL connection
func (m *MySQL) Close() {
	m.db.Close()
}

// Get the value at "key", returns the last modified index
// to use in conjunction to CAS calls
func (m *MySQL) Get(key string) (*store.KVPair, error) {
	args := make([]interface{}, 0, MaxFields)
	for _, field := range splitFields(key, true) {
		args = append(args, field)
	}

	row := m.db.QueryRow(
		fmt.Sprintf("SELECT `last_index`, `value` FROM `%s` WHERE %s LIMIT 1;",
			m.table, whereCond(key, true)), args...)

	var (
		value []byte
		index uint64
	)

	if err := row.Scan(&index, &value); err != nil {
		if err == sql.ErrNoRows {
			err = store.ErrKeyNotFound
		}
		return nil, err
	}

	return &store.KVPair{Key: key, Value: value, LastIndex: index}, nil
}

// Put a value at "key". We cannot guarantee the systime on each machine are
// the same, hence it not support TTL.
func (m *MySQL) Put(key string, value []byte, _ *store.WriteOptions) error {
	now := time.Now()
	args := make([]interface{}, 0, MaxFields+6)
	for _, field := range splitFields(key, true) {
		args = append(args, field)
	}
	args = append(args, uint64(1), value, now, now, value, now)

	result, err := m.db.Exec(
		fmt.Sprintf("INSERT INTO `%s` (%s, `last_index`, `value`, `create_at`, `update_at`) VALUES(%s?,?,?,?) ON DUPLICATE KEY UPDATE `last_index`=`last_index`+1, `value`=?, `update_at`=?;",
			m.table, eachFields(), strings.Repeat("?,", MaxFields)), args...)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return ErrPutMissing
	}
	return nil
}

// Exists checks that the key exists inside the store
func (m *MySQL) Exists(key string) (bool, error) {
	_, err := m.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Delete a value at "key"
func (m *MySQL) Delete(key string) error {
	args := make([]interface{}, 0, MaxFields)
	for _, field := range splitFields(key, true) {
		args = append(args, field)
	}

	result, err := m.db.Exec(
		fmt.Sprintf("DELETE FROM `%s` WHERE %s;", m.table, whereCond(key, true)),
		args...)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return store.ErrKeyNotFound
	}
	return nil
}

// List child nodes of a given directory
func (m *MySQL) List(directory string) ([]*store.KVPair, error) {
	args := make([]interface{}, 0, MaxFields)
	for _, field := range splitFields(directory, false) {
		args = append(args, field)
	}

	rows, err := m.db.Query(
		fmt.Sprintf("SELECT %s, `last_index`, `value` FROM `%s` WHERE %s;",
			eachFields(), m.table, whereCond(directory, false)),
		args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var pairs []*store.KVPair

	for rows.Next() {
		var (
			fields = make([]string, MaxFields)
			inf    = make([]interface{}, 0, MaxFields+2)
			value  []byte
			index  uint64
		)

		for i := 0; i < len(fields); i++ {
			inf = append(inf, &fields[i])
		}
		inf = append(inf, &index, &value)

		if err := rows.Scan(inf...); err != nil {
			return pairs, err
		}

		pairs = append(pairs, &store.KVPair{
			Key:       path.Join(fields...),
			Value:     value,
			LastIndex: index,
		})
	}

	if err := rows.Err(); err != nil {
		return pairs, err
	}

	if len(pairs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	return pairs, nil
}

// DeleteTree deletes a range of keys under a given directory
func (m *MySQL) DeleteTree(directory string) error {
	args := make([]interface{}, 0, MaxFields)
	for _, field := range splitFields(directory, false) {
		args = append(args, field)
	}

	result, err := m.db.Exec(
		fmt.Sprintf("DELETE FROM `%s` WHERE %s;", m.table, whereCond(directory, false)),
		args...)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return store.ErrKeyNotFound
	}
	return nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (m *MySQL) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	// Get the key first, and check the key is exist.
	pair, err := m.Get(key)
	if err != nil && err != store.ErrKeyNotFound {
		return nil, err
	}

	watchCh := make(chan *store.KVPair, 1)
	lastIndex := uint64(0)
	if pair != nil {
		lastIndex = pair.LastIndex
	}
	watchCh <- pair

	go func() {
		defer close(watchCh)

		tick := time.NewTicker(DefaultWatchWaitTime)
		defer tick.Stop()

		for {
			// Check if we should quit
			select {
			case <-tick.C:
			case <-stopCh:
				return
			}

			// Get the key
			pair, err := m.Get(key)
			if err != nil && err != store.ErrKeyNotFound {
				// keep the same behavior with other backend implementant.
				return
			}

			index := uint64(0)
			if pair != nil {
				index = pair.LastIndex
			}

			// If index didn't change then it means `Get` returned
			// because of the WaitTime and the key didn't changed.
			if lastIndex == index {
				continue
			}
			lastIndex = index
			select {
			case watchCh <- pair:
			case <-stopCh:
				return
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (m *MySQL) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	directory = normalize(directory)
	list, err := m.List(directory)
	if err != nil && err != store.ErrKeyNotFound {
		return nil, err
	}

	// record each children's index
	indice := make(map[string]uint64)
	watchCh := make(chan []*store.KVPair, 1)
	for _, p := range list {
		indice[p.Key] = p.LastIndex
	}

	watchCh <- list

	go func() {
		tick := time.NewTicker(DefaultWatchWaitTime)
		defer func() {
			tick.Stop()
			close(watchCh)
		}()

		for {
			// Check if we should quit
			select {
			case <-tick.C:
			case <-stopCh:
				return
			}

			// Get all the childrens
			list, err := m.List(directory)
			if err != nil && err != store.ErrKeyNotFound {
				return
			}

			var (
				changed bool
				pairs   []*store.KVPair
				exists  = make(map[string]struct{})
			)

			for _, p := range list {
				exists[p.Key] = struct{}{}
				if p.Key == directory {
					continue
				}
				pairs = append(pairs, p)
				// If LastIndex didn't change then it means `Get` returned
				// because of the WaitTime and the child keys didn't change.
				if p.LastIndex != indice[p.Key] {
					changed = true
				}
				indice[p.Key] = p.LastIndex
			}

			// find someone has been deleted
			for key := range indice {
				if _, ok := exists[key]; !ok {
					changed = true
					delete(indice, key)
				}
			}

			if changed {
				select {
				case watchCh <- pairs:
				case <-stopCh:
					return
				}
			}
		}
	}()

	return watchCh, nil
}

// AtomicPut put a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (m *MySQL) AtomicPut(key string, value []byte, previous *store.KVPair,
	_ *store.WriteOptions) (bool, *store.KVPair, error) {
	var (
		result sql.Result
		err    error
	)

	now := time.Now()
	if previous == nil {
		args := make([]interface{}, 0, MaxFields+4)
		for _, field := range splitFields(key, true) {
			args = append(args, field)
		}
		args = append(args, uint64(1), value, now, now)
		result, err = m.db.Exec(
			fmt.Sprintf("INSERT INTO `%s` (%s, `last_index`, `value`, `create_at`, `update_at`) VALUES(%s?,?,?,?);",
				m.table, eachFields(), strings.Repeat("?,", MaxFields)),
			args...)
	} else {
		args := make([]interface{}, 0, MaxFields+3)
		args = append(args, value, now)
		for _, field := range splitFields(key, true) {
			args = append(args, field)
		}
		args = append(args, previous.LastIndex)
		result, err = m.db.Exec(
			fmt.Sprintf("UPDATE `%s` SET `value`=?, `last_index`=`last_index`+1, `update_at`=? WHERE %s AND `last_index`=?;",
				m.table, whereCond(key, true)),
			args...)
	}

	if err != nil {
		if merr, ok := err.(*mysql.MySQLError); ok && merr.Number == 1062 {
			return false, nil, store.ErrKeyExists
		}
		return false, nil, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, nil, err
	}

	if affected == 0 {
		return false, nil, store.ErrKeyModified
	}

	pair, err := m.Get(key)
	if err != nil {
		return false, nil, err
	}

	return true, pair, nil
}

// AtomicDelete deletes a value at "key" if the key has not
// been modified in the meantime, throws an error if this is the case
func (m *MySQL) AtomicDelete(key string, previous *store.KVPair) (ok bool, err error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	// Extra Get operation to check on the key
	if _, err := m.Get(key); err != nil {
		return false, err
	}

	args := make([]interface{}, 0, MaxFields+1)
	for _, field := range splitFields(key, true) {
		args = append(args, field)
	}
	args = append(args, previous.LastIndex)

	result, err := m.db.Exec(
		fmt.Sprintf("DELETE FROM `%s` WHERE %s AND `last_index`=?;",
			m.table, whereCond(key, true)),
		args...)
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if affected == 0 {
		return false, store.ErrKeyModified
	}

	return true, nil
}

// NewLock creates a lock for a given key.
// The returned Locker is not held and must be acquired
// with `.Lock`. The Value is optional.
// https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html
func (m *MySQL) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	var (
		value   []byte
		ttl     = defaultLockTTL
		renewCh = make(chan struct{})
	)

	if options != nil {
		if options.TTL != 0 {
			ttl = options.TTL
		}
		if options.Value != nil {
			value = options.Value
		}
		if options.RenewLock != nil {
			renewCh = options.RenewLock
		}
	}

	return &mysqlLock{
		key:     normalize(key),
		session: session(),
		value:   value,
		ttl:     ttl,
		renewCh: renewCh,
		m:       m,
	}, nil
}

type mysqlLock struct {
	mu       sync.Mutex
	wg       sync.WaitGroup
	key      string
	session  string
	value    []byte
	ttl      time.Duration
	m        *MySQL
	unlockCh chan struct{}
	renewCh  chan struct{}
}

func (l *mysqlLock) acquire(lastIndex uint64, expired bool) (index uint64, ok bool, err error) {
	var (
		tx       *sql.Tx
		result   sql.Result
		affected int64
		session  string
	)

	if tx, err = l.m.db.Begin(); err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	args := make([]interface{}, 0, MaxFields)
	for _, field := range splitFields(l.key, true) {
		args = append(args, field)
	}

	// lock the mysql row
	row := tx.QueryRow(
		fmt.Sprintf("SELECT `lock_session`, `last_index` FROM `%s` WHERE %s LIMIT 1 FOR UPDATE;",
			l.m.table, whereCond(l.key, true)),
		args...)

	if err = row.Scan(&session, &index); err != nil && err != sql.ErrNoRows {
		return
	}

	now := time.Now()

	switch session {
	case "":
		// the lock key is not exist, so create one
		args := make([]interface{}, 0, MaxFields+5)
		for _, field := range splitFields(l.key, true) {
			args = append(args, field)
		}
		args = append(args, l.session, uint64(1), l.value, now, now)
		result, err = tx.Exec(
			fmt.Sprintf("INSERT INTO `%s` (%s,`lock_session`, `last_index`, `value`, `create_at`, `update_at`) VALUES(%s?,?,?,?,?);",
				l.m.table, eachFields(), strings.Repeat("?,", MaxFields)),
			args...)
	case l.session:
		// the current session is same with us, we fetch this lock directly
		args := make([]interface{}, 0, MaxFields+2)
		args = append(args, l.value, now)
		for _, field := range splitFields(l.key, true) {
			args = append(args, field)
		}
		result, err = tx.Exec(
			fmt.Sprintf("UPDATE `%s` SET `last_index`=`last_index`+1, `value`=?, `update_at`=? WHERE %s;",
				l.m.table, whereCond(l.key, true)),
			args...)
	default:
		if !expired || lastIndex != index {
			// someone is helding the lock
			return
		}
		// someone lost this lock
		args := make([]interface{}, 0, MaxFields+3)
		args = append(args, l.session, l.value, now)
		for _, field := range splitFields(l.key, true) {
			args = append(args, field)
		}
		result, err = tx.Exec(
			fmt.Sprintf("UPDATE `%s` SET `lock_session`=?, `last_index`=`last_index`+1, `value`=?, `update_at`=? WHERE %s;",
				l.m.table, whereCond(l.key, true)),
			args...)
	}

	if err != nil {
		if merr, ok := err.(*mysql.MySQLError); ok && merr.Number == 1062 {
			err = nil // ignore insert failed on duplicate entry
		}
		return
	}

	affected, err = result.RowsAffected()
	if err != nil {
		return
	}

	if affected != 0 {
		ok = true
		index++
	}
	return
}

func (l *mysqlLock) renewLock() (err error) {
	var (
		tx       *sql.Tx
		affected int64
		session  string
		result   sql.Result
	)

	tx, err = l.m.db.Begin()
	if err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	args := make([]interface{}, 0, MaxFields)
	for _, field := range splitFields(l.key, true) {
		args = append(args, field)
	}

	row := tx.QueryRow(
		fmt.Sprintf("SELECT `lock_session` FROM `%s` WHERE %s LIMIT 1 FOR UPDATE;",
			l.m.table, whereCond(l.key, true)),
		args...)
	if err = row.Scan(&session); err != nil {
		return
	}

	if session != l.session {
		return ErrLockLost
	}

	args = args[:0]
	args = append(args, time.Now())
	for _, field := range splitFields(l.key, true) {
		args = append(args, field)
	}

	result, err = tx.Exec(
		fmt.Sprintf("UPDATE `%s` SET `last_index`=`last_index`+1, `update_at`=? WHERE %s;",
			l.m.table, whereCond(l.key, true)),
		args...)
	if err != nil {
		return
	}
	affected, err = result.RowsAffected()
	if err != nil {
		return
	}
	if affected == 0 {
		err = ErrLockLost
	}
	return
}

func (l *mysqlLock) holdLock(lockHeld chan struct{}, stopLocking, unlock <-chan struct{}) {
	tick := time.NewTicker(l.ttl / defaultTTLPeriod)

	defer func() {
		close(lockHeld)
		tick.Stop()
		l.wg.Done()
	}()

	for {
		select {
		case <-tick.C:
			if err := l.renewLock(); err != nil && err != driver.ErrBadConn {
				return
			}
		case <-unlock:
			return
		case <-stopLocking:
			return
		}
	}
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *mysqlLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var (
		expired   bool
		count     int
		lastIndex uint64
		ttl       = l.ttl / defaultTTLPeriod
	)

	// this lock.Lock is already invoked
	if l.unlockCh != nil {
		return nil, store.ErrCannotLock
	}

	lockHeld := make(chan struct{})
	unlockCh := make(chan struct{})
	tick := time.NewTimer(0)

	defer tick.Stop()

	for {
		select {
		case <-tick.C:
		case <-stopChan:
			return nil, ErrAbortTryLock
		}

		index, ok, err := l.acquire(lastIndex, expired)
		if err == nil {
			if ok {
				l.wg.Add(1)
				l.unlockCh = unlockCh
				go l.holdLock(lockHeld, l.renewCh, unlockCh)
				return lockHeld, nil
			} else if index == lastIndex {
				count++
				if count >= defaultTTLPeriod-1 {
					expired = true
				}
			} else {
				lastIndex = index
				expired = false
			}
		} else if err != nil && err != driver.ErrBadConn && err != mysql.ErrInvalidConn {
			return nil, err
		}
		tick.Reset(ttl)
	}
}

// Unlock the "key".
func (l *mysqlLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.unlockCh != nil {
		close(l.unlockCh)
		l.wg.Wait()
		l.unlockCh = nil

		// delete the lock key
		args := make([]interface{}, 0, MaxFields+1)
		for _, field := range splitFields(l.key, true) {
			args = append(args, field)
		}
		args = append(args, l.session)
		result, err := l.m.db.Exec(
			fmt.Sprintf("DELETE FROM `%s` WHERE %s AND `lock_session`=?;",
				l.m.table, whereCond(l.key, true)),
			args...)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return ErrLockLost
		}
	}
	return nil
}
