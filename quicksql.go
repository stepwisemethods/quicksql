package quicksql

import (
	"database/sql"
	"errors"
	"strconv"
)

var (
	ErrNullValue        = errors.New("null value encountered")
	ErrInvalidColumn    = errors.New("invalid column")
	ErrUnsupportedValue = errors.New("unsupported value for casting")
)

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

func (s *Session) Select(query string, args ...interface{}) ([]*Record, error) {
	rows, err := s.db.Query(query, args...)
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
			values: map[string]interface{}{},
			fields: colNames,
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

type Record struct {
	fields []string
	values map[string]interface{}
}

func (r *Record) Fields() []string {
	// TODO should we copy?
	return r.fields
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
