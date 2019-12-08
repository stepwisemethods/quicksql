# quicksql

QuickSQL is a library for accessing data in SQL databases. The primary goal is
to make it possible for developers to quickly get to the data without having to
define Go structs or implement custom scanners.

## Example

```go
// here connection is *sql.DB
session := quicksql.NewSession(connection)
rows, err := session.Select("SELECT id, name FROM users LIMIT 10")
// rows will have up to 10 records
for _, row := range rows {
    // reads id and name columns
    println("ID: ", row.MustString("id"), "Name: ", row.MustString("name"))
}
```

## CRUD

Since QuickSQL records are not backend by concrete Go structs, CRUD operations
are only allowed on records that originated from queries against a single
database table. If record includes columns from several fields (e.g. JOIN
statement), CRUD operations will fail with a database error.

### Create database record

TODO not implemented

### Read database records

```go
session := quicksql.NewSession(connection)
rows, err := session.Select("SELECT * FROM users WHERE email = ?",
    quicksql.ArgsOption("john@example.com"),
    quicksql.TableOption("users"),
    quicksql.PrimaryKeyOption("id"),
)
```

To read records we need to call `Select` on the QuickSQL session. You must pass
`TableOption` and `PrimaryKeyOption` if you are planning to update or delete
returned records later.

### Update record

```go
session := quicksql.NewSession(connection)
rows, err := session.Select("SELECT * FROM users WHERE email = ?",
    quicksql.ArgsOption("john@example.com"),
    quicksql.TableOption("users"),
    quicksql.PrimaryKeyOption("id"),
)

record := rows[0]
record.Set("name", "John Doe")
record.Set("email", "newemail@example.com")
err = session.Save(record)
```

To save an existing record, the only thing you have to do is pass it to the
`Save` method of the QuickSQL session instance. This operation will only work
if the record was previously created with `TableOption` and `PrimaryKeyOption`
options.

### Delete record

```go
session := quicksql.NewSession(connection)
rows, err := session.Select("SELECT * FROM users WHERE email = ?",
    quicksql.ArgsOption("john@example.com"),
    quicksql.TableOption("users"),
    quicksql.PrimaryKeyOption("id"),
)

err = session.Delete(rows[0])
```

To delete an existing record you need to pass it to the `Delete` methods of
QuickSQL session instance.

## TODO

- Add support for time.Time and ParseTime setting in mysql driver.
- Add support for record creation.
- Reading a field after `Set` won't work since getter methods assume []uint8 slices as values.
- Setup CI on Github.

## License

The quicksql library is open-sourced software and licensed under the [MIT license](LICENSE)
