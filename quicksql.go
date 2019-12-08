package quicksql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrNullValue         = errors.New("quicksql: null value encountered")
	ErrInvalidColumn     = errors.New("quicksql: invalid column")
	ErrUnsupportedValue  = errors.New("quicksql: unsupported value for casting")
	ErrPrimaryKeyNotSet  = errors.New("quicksql: primary key not set")
	ErrPrimaryKeyInvalid = errors.New("quicksql: invalid primary key")
	ErrTableNotSet       = errors.New("quicksql: table not set")
)

type sessionContext struct {
	// arguments to pass to the query
	args []interface{}
	// primary key to set on the record if any
	pk []string
	// table name we're currently working on
	tableName string
	// flag indicating whether the table we're working with
	// has an auto incrementing PK
	autoIncrement bool
}

type SessionOption func(ctx *sessionContext) error

func PrimaryKeyOption(pk ...string) SessionOption {
	return func(ctx *sessionContext) error {
		ctx.pk = pk
		return nil
	}
}

func AutoIncrementOption() SessionOption {
	return func(ctx *sessionContext) error {
		ctx.autoIncrement = true
		return nil
	}
}

func ArgsOption(args ...interface{}) SessionOption {
	return func(ctx *sessionContext) error {
		ctx.args = args
		return nil
	}
}

func TableOption(name string) SessionOption {
	return func(ctx *sessionContext) error {
		ctx.tableName = name
		return nil
	}
}

type SqlInterface interface {
	Query(string, ...interface{}) (*sql.Rows, error)
	Exec(string, ...interface{}) (sql.Result, error)
}

type Session struct {
	db SqlInterface
}

func NewSession(db SqlInterface) *Session {
	return &Session{
		db: db,
	}
}

func (s *Session) Select(query string, options ...SessionOption) ([]*Record, error) {
	selectCtx := &sessionContext{
		args: []interface{}{},
		pk:   []string{},
	}

	for _, option := range options {
		if err := option(selectCtx); err != nil {
			return nil, err
		}
	}

	rows, err := s.db.Query(query, selectCtx.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	records := []*Record{}

	for rows.Next() {
		cols := make([]interface{}, len(colNames))
		colPtrs := make([]interface{}, len(colNames))
		for i := 0; i < len(colNames); i++ {
			colPtrs[i] = &cols[i]
		}

		if err := rows.Scan(colPtrs...); err != nil {
			return nil, err
		}

		record := NewRecord(TableOption(selectCtx.tableName), PrimaryKeyOption(selectCtx.pk...))
		for i, col := range cols {
			record.Set(colNames[i], col)
		}

		records = append(records, record)

	}
	return records, nil
}

func (s *Session) Create(record *Record) error {
	if record.tableName == "" {
		return ErrTableNotSet
	}

	fields := []string{}
	args := []interface{}{}
	for field, value := range record.values {
		fields = append(fields, "`"+field+"`")
		args = append(args, value)
	}

	argPlaceholders := make([]string, len(args))
	for i := range argPlaceholders {
		argPlaceholders[i] = "?"
	}

	query := "INSERT INTO " + record.tableName + " (" + strings.Join(fields, ", ") + ") VALUES(" + strings.Join(argPlaceholders, ", ") + ")"

	res, err := s.db.Exec(query, args...)
	if err != nil {
		return err
	}

	if len(record.pk) == 1 && record.autoIncrement {
		// When a non-composite primary key is set and the value for the PK was not set
		// as part of the create operation, then assume that we're working with auto incrementing table
		// and try to read the last insert id into PK field.
		lastid, err := res.LastInsertId()
		if err == nil {
			record.Set(record.pk[0], lastid)
		} else {
			// TODO we're silently skipping here, we might want to do something about it in the future.
		}
	}
	return nil
}

func (s *Session) Save(record *Record) error {
	args := []interface{}{}
	pkFields := []string{}
	fields := []string{}

	if err := validateRecordForUpdateOrDelete(record); err != nil {
		return err
	}

	for _, field := range record.pk {
		pkFields = append(pkFields, "`"+field+"` = ?")
	}

	for field, value := range record.values {
		fields = append(fields, "`"+field+"` = ?")
		args = append(args, value)
	}

	for _, pkField := range record.pk {
		pkValue, ok := record.values[pkField]
		if !ok {
			return ErrPrimaryKeyInvalid
		}
		args = append(args, pkValue)
	}

	query := "UPDATE " + record.tableName + " SET " + strings.Join(fields, ", ") + " WHERE " + strings.Join(pkFields, " AND ") + " LIMIT 1"

	_, err := s.db.Exec(query, args...)
	return err
}

func (s *Session) Delete(record *Record) error {
	if err := validateRecordForUpdateOrDelete(record); err != nil {
		return err
	}

	args := []interface{}{}
	pkFields := []string{}

	for _, field := range record.pk {
		pkFields = append(pkFields, "`"+field+"` = ?")
		pkValue, ok := record.values[field]
		if !ok {
			return ErrPrimaryKeyInvalid
		}
		args = append(args, pkValue)
	}

	query := "DELETE FROM " + record.tableName + " WHERE " + strings.Join(pkFields, " AND ") + " LIMIT 1"
	_, err := s.db.Exec(query, args...)
	return err
}

type Record struct {
	values        map[string][]byte
	pk            []string
	tableName     string
	autoIncrement bool
}

func NewRecord(options ...SessionOption) *Record {
	ctx := &sessionContext{
		pk: []string{},
	}

	for _, option := range options {
		if err := option(ctx); err != nil {
			// TODO not a big fan of this, but let's assume people are not doing silly things.
			panic(err)
		}
	}

	record := &Record{
		pk:            ctx.pk,
		tableName:     ctx.tableName,
		values:        map[string][]byte{},
		autoIncrement: ctx.autoIncrement,
	}

	return record
}

func (r *Record) Fields() []string {
	fields := []string{}

	for k, _ := range r.values {
		fields = append(fields, k)
	}

	return fields
}

func (r *Record) Set(name string, value interface{}) error {
	// When setting a value on the record it will be converted to a uint8
	// slice. All Record getters currently assume that data coming from the
	// database is passed as uint8 slice. This behavior allows us reading
	// values from the record after setting them.

	switch v := value.(type) {
	case string:
		r.values[name] = []uint8(v)
		return nil
	case []byte:
		r.values[name] = v
		return nil
	case nil:
		r.values[name] = nil
		return nil
	}

	byteSlice := []uint8(fmt.Sprintf("%v", value))
	r.values[name] = byteSlice
	return nil
}

func (r *Record) String(name string) (string, error) {
	v, ok := r.values[name]
	if !ok {
		return "", ErrInvalidColumn
	}

	if v == nil {
		return "", ErrNullValue
	}

	return string(v), nil
}

func (r *Record) MustString(name string) string {
	value, err := r.String(name)
	if err != nil {
		panic(err)
	}
	return value
}

func (r *Record) UInt64(name string) (uint64, error) {
	v, ok := r.values[name]
	if !ok {
		return 0, ErrInvalidColumn
	}

	if v == nil {
		return 0, ErrNullValue
	}

	number, err := strconv.ParseUint(string(v), 10, 64)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func (r *Record) MustUInt64(name string) uint64 {
	v, err := r.UInt64(name)
	if err != nil {
		panic(err)
	}
	return v
}

func (r *Record) Int64(name string) (int64, error) {
	v, ok := r.values[name]
	if !ok {
		return 0, ErrInvalidColumn
	}

	if v == nil {
		return 0, ErrNullValue
	}

	number, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func (r *Record) MustInt64(name string) int64 {
	v, err := r.Int64(name)
	if err != nil {
		panic(err)
	}
	return v
}

func validateRecordForUpdateOrDelete(record *Record) error {
	if record.tableName == "" {
		return ErrTableNotSet
	}

	if record.pk == nil || len(record.pk) == 0 {
		return ErrPrimaryKeyNotSet
	}

	return nil
}
