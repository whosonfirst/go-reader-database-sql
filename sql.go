package reader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	wof_reader "github.com/whosonfirst/go-reader"
	"io"
	"io/ioutil"
	_ "log"
	"net/url"
	"strings"
)

func init() {
	r := NewSQLReader()
	wof_reader.Register("sql", r)
}

type SQLReader struct {
	wof_reader.Reader
	conn  *sql.DB
	table string
	key   string
	value string
}

func NewSQLReader() wof_reader.Reader {

	r := SQLReader{}
	return &r
}

// sql://sqlite/geojson/id/body/?dsn=....

func (r *SQLReader) Open(ctx context.Context, uri string) error {

	u, err := url.Parse(uri)

	if err != nil {
		return err
	}

	q := u.Query()

	driver := u.Host
	path := u.Path

	path = strings.TrimLeft(path, "/")
	parts := strings.Split(path, "/")

	if len(parts) != 3 {
		return errors.New("Invalid path")
	}

	table := parts[0]
	key := parts[1]
	value := parts[2]

	dsn := q.Get("dsn")

	if dsn == "" {
		return errors.New("Missing dsn parameter")
	}

	conn, err := sql.Open(driver, dsn)

	if err != nil {
		return err
	}

	// check for table, etc. here?

	r.conn = conn
	r.table = table
	r.key = key
	r.value = value

	return nil
}

func (r *SQLReader) Read(ctx context.Context, uri string) (io.ReadCloser, error) {

	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s=?", r.value, r.table, r.key)

	row := r.conn.QueryRowContext(ctx, q, uri)

	var body string
	err := row.Scan(&body)

	if err != nil {
		return nil, err
	}

	sr := strings.NewReader(body)
	fh := ioutil.NopCloser(sr)

	return fh, nil
}
