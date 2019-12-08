package quicksql

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func openMySQL(t *testing.T) *sql.DB {
	cfg := mysql.Config{
		User:                 "root",
		Passwd:               "pass",
		Net:                  "tcp",
		Addr:                 "127.0.0.1:32768",
		AllowNativePasswords: true,
		DBName:               "test",
		ParseTime:            false,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("error connecting to the database: %s", err.Error())
		return nil
	}
	return db
}

func createTables(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS test_table"); err != nil {
		return err
	}

	createTableStatement := `
		CREATE TABLE test_table (
			id INT(11) NOT NULL AUTO_INCREMENT,
			field_string VARCHAR(255) NOT NULL,
			field_string_nullable VARCHAR(255),
			field_integer INT(11) NOT NULL,
			field_integer_nullable INT(11),
			field_binary BINARY(10) NOT NULL,
			field_binary_null BINARY(64),
			field_datetime DATETIME NOT NULL,
			field_datetime_nullable DATETIME,
			field_text TEXT,
			field_decimal DECIMAL(12, 2) NOT NULL,
			field_decimal_nullable DECIMAL(12, 2),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	`

	if _, err := db.Exec(createTableStatement); err != nil {
		return err
	}

	datevalue := time.Date(2020, time.February, 33, 15, 30, 44, 0, time.UTC)

	record := map[string]interface{}{
		"field_string":            "field_string",
		"field_string_nullable":   nil,
		"field_integer":           666,
		"field_integer_nullable":  nil,
		"field_binary":            "binary",
		"field_binary_null":       nil,
		"field_datetime":          datevalue,
		"field_datetime_nullable": nil,
		"field_text":              "text",
		"field_decimal":           555.66,
		"field_decimal_nullable":  nil,
	}

	fields := []string{}
	valuePlaceholders := []string{}
	values := []interface{}{}
	for key, value := range record {
		fields = append(fields, key)
		valuePlaceholders = append(valuePlaceholders, "?")
		values = append(values, value)
	}

	query := "INSERT INTO test_table (" + strings.Join(fields, ",") + ") VALUES(" + strings.Join(valuePlaceholders, ",") + ")"

	if _, err := db.Exec(query, values...); err != nil {
		return err
	}

	return nil
}

func TestFieldNames(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select("SELECT id, field_decimal as alias FROM test_table")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	assert.Equal(t, []string{"id", "alias"}, rows[0].Fields())
}

func TestSelectWithOptions(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select(
		"SELECT id, field_decimal as alias FROM test_table WHERE field_integer = ? AND field_string = ?",
		ArgsOption(666, "field_string"),
		PrimaryKeyOption("id"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	assert.Equal(t, []string{"id", "alias"}, rows[0].Fields())
}

func TestSaveRecord(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select(
		"SELECT * FROM test_table LIMIT 1",
		PrimaryKeyOption("id"),
		TableOption("test_table"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	record := rows[0]
	assert.NoError(t, record.Set("field_string", "new value"))
	assert.NoError(t, session.Save(record))

	rows, err = session.Select("SELECT * FROM test_table LIMIT 1")
	assert.NoError(t, err)
	assert.Equal(t, "new value", rows[0].MustString("field_string"))
}

func TestDeleteRecord(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select(
		"SELECT * FROM test_table LIMIT 1",
		PrimaryKeyOption("id"),
		TableOption("test_table"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	assert.NoError(t, session.Delete(rows[0]))

	rows, err = session.Select("SELECT * FROM test_table LIMIT 1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))
}

func TestSaveRecordCompositeKey(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select(
		"SELECT * FROM test_table LIMIT 1",
		PrimaryKeyOption("id", "field_integer"),
		TableOption("test_table"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	record := rows[0]
	assert.NoError(t, record.Set("field_string", "new value"))
	assert.NoError(t, session.Save(record))

	rows, err = session.Select("SELECT * FROM test_table LIMIT 1")
	assert.NoError(t, err)
	assert.Equal(t, "new value", rows[0].MustString("field_string"))
}

func TestStringRead(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select("SELECT * FROM test_table")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	tests := []struct {
		field         string
		expectedValue string
	}{
		{
			field:         "field_string",
			expectedValue: "field_string",
		},
		{
			field:         "field_integer",
			expectedValue: "666",
		},
		{
			field:         "field_binary",
			expectedValue: "binary\x00\x00\x00\x00",
		},
		{
			field:         "field_text",
			expectedValue: "text",
		},
		{
			field:         "field_decimal",
			expectedValue: "555.66",
		},
		// With parse time disabled
		{
			field:         "field_datetime",
			expectedValue: "2020-03-04 15:30:44",
		},
	}

	record := rows[0]
	for _, test := range tests {
		value, err := record.String(test.field)
		assert.NoError(t, err)
		assert.Equal(t, test.expectedValue, value)
	}
}

func TestReadInteger(t *testing.T) {
	db := openMySQL(t)
	defer db.Close()

	if err := createTables(db); err != nil {
		t.Fatalf(err.Error())
	}

	session := NewSession(db)
	rows, err := session.Select("SELECT field_integer FROM test_table")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))

	record := rows[0]

	intval, err := record.Int64("field_integer")
	assert.NoError(t, err)
	assert.Equal(t, int64(666), intval)

	uintval, err := record.UInt64("field_integer")
	assert.NoError(t, err)
	assert.Equal(t, uint64(666), uintval)
}
