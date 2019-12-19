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
	"regexp"
	"strings"
)

type readFunc func(string) (string, error)

var VALID_TABLE *regexp.Regexp
var VALID_KEY *regexp.Regexp
var VALID_VALUE *regexp.Regexp

var URI_READFUNC readFunc

func init() {

	VALID_TABLE = regexp.MustCompile(`^[a-zA-Z0-9-_]+$`)
	VALID_KEY = regexp.MustCompile(`^[a-zA-Z0-9-_]+$`)
	VALID_VALUE = regexp.MustCompile(`^[a-zA-Z0-9-_]+$`)

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

	if !VALID_TABLE.MatchString(table) {
		return errors.New("Invalid table")
	}

	if !VALID_KEY.MatchString(key) {
		return errors.New("Invalid key")
	}

	if !VALID_VALUE.MatchString(value) {
		return errors.New("Invalid value")
	}

	r.conn = conn
	r.table = table
	r.key = key
	r.value = value

	return nil
}

func (r *SQLReader) Read(ctx context.Context, uri string) (io.ReadCloser, error) {

	if URI_READFUNC != nil {

		new_uri, err := URI_READFUNC(uri)

		if err != nil {
			return nil, err
		}

		uri = new_uri
	}

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
