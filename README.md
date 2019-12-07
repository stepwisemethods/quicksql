# quicksql

QuickSQL is a library for accessing data in SQL databases. The primary goal is
to make it possible for developers to quickly get to the data without having to
define Go structs or implement custom scanners.

## Example

```go
session := quicksql.NewSession(connection)
rows, err := session.Select("SELECT id, name FROM users LIMIT 10")
// rows will have up to 10 records
for _, row := range rows {
    // reads id and name columns
    println("ID: ", row.MustString("id"), "Name: ", row.MustString("name"))
}
```

## TODO

- [ ] Add support for time.Time and ParseTime setting in mysql driver.
- [ ] Add support for saving records back to the database.

## License

The quicksql library is open-sourced software and licensed under the [MIT license](LICENSE)
