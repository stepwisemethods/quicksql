package quicksql

import (
	"database/sql"
	"errors"
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
	// table name we're currenctly working on
	tableName string
}

type SelectOption func(ctx *sessionContext) error

func PrimaryKeyOption(pk ...string) SelectOption {
	return func(ctx *sessionContext) error {
		ctx.pk = pk
		return nil
	}
}

func ArgsOption(args ...interface{}) SelectOption {
	return func(ctx *sessionContext) error {
		ctx.args = args
		return nil
	}
}

func TableOption(name string) SelectOption {
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

func (s *Session) Select(query string, options ...SelectOption) ([]*Record, error) {
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
		record := &Record{
			values:    map[string]interface{}{},
			fields:    colNames,
			pk:        selectCtx.pk,
			tableName: selectCtx.tableName,
		}

		cols := make([]interface{}, len(colNames))
		colPtrs := make([]interface{}, len(colNames))
		for i := 0; i < len(colNames); i++ {
			colPtrs[i] = &cols[i]
		}

		if err := rows.Scan(colPtrs...); err != nil {
			return nil, err
		}

		for i, col := range cols {
			record.values[colNames[i]] = col
		}

		records = append(records, record)
	}
	return records, nil
}

func (s *Session) Save(record *Record) error {
	args := []interface{}{}
	pkFields := []string{}
	fields := []string{}

	if record.tableName == "" {
		return ErrTableNotSet
	}

	if record.pk == nil || len(record.pk) == 0 {
		return ErrPrimaryKeyNotSet
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
	if err != nil {
		return err
	}

	return nil
}

type Record struct {
	fields    []string
	values    map[string]interface{}
	pk        []string
	tableName string
}

func (r *Record) Fields() []string {
	// TODO should we copy?
	return r.fields
}

func (r *Record) Set(name string, value interface{}) error {
	r.values[name] = value
	return nil
}

func (r *Record) String(name string) (string, error) {
	v, ok := r.values[name]
	if !ok {
		return "", ErrInvalidColumn
	}

	switch value := v.(type) {
	case nil:
		return "", ErrNullValue
	case []uint8:
		return string(value), nil
	default:
		return "", ErrUnsupportedValue
	}
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

	switch value := v.(type) {
	case nil:
		return 0, ErrNullValue
	case []uint8:
		number, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return 0, err
		}
		return number, nil
	default:
		return 0, ErrUnsupportedValue
	}
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

	switch value := v.(type) {
	case nil:
		return 0, ErrNullValue
	case []uint8:
		number, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, err
		}
		return number, nil
	default:
		return 0, ErrUnsupportedValue
	}
}

func (r *Record) MustInt64(name string) int64 {
	v, err := r.Int64(name)
	if err != nil {
		panic(err)
	}
	return v
}