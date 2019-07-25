package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// JSON type
type JSON map[string]interface{}

// DataItem struct
type DataItem struct {
	ID        int64
	Namespace string
	Key       string
	Version   int64
	Content   JSON
	EventTime time.Time
}

// NamespaceMostRecentID struct
type NamespaceMostRecentID struct {
	Namespace    string
	MostRecentID int64
}

// DataItemStore interface
type DataItemStore interface {
	Store(ns, key string, content JSON) (*DataItem, error)
	HistoryOf(ns, key string) ([]DataItem, error)
	ListItems(ns, key string, iterStartID int64, iterSize int) ([]DataItem, error)
	ListByAuthor(ns, key, author string, iterStartID int64, iterSize int) ([]DataItem, error)
	ListNamespaces(iterStartID int64, iterSize int) ([]NamespaceMostRecentID, error)
}

type postgresDataItemStore struct {
	db *sql.DB
}

// NewDataItemStore function
func NewDataItemStore(connStr string) (DataItemStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	store := postgresDataItemStore{db}
	return store, nil
}

func rowToDataItem(row *sql.Row) (*DataItem, error) {
	var dataItem DataItem
	var encodedContent []byte
	err := row.Scan(
		&dataItem.ID,
		&dataItem.Namespace,
		&dataItem.Key,
		&dataItem.Version,
		&encodedContent,
		&dataItem.EventTime)
	if err != nil {
		return nil, err
	}
	var decodedContent JSON
	err = json.Unmarshal(encodedContent, &decodedContent)
	if err != nil {
		return nil, err
	}
	dataItem.Content = decodedContent
	return &dataItem, nil
}

func rowsToDataItems(rows *sql.Rows, initCap int) ([]DataItem, error) {
	dataItems := make([]DataItem, 0, initCap)
	for rows.Next() {
		var dataItem DataItem
		var encodedContent []byte
		err := rows.Scan(
			&dataItem.ID,
			&dataItem.Namespace,
			&dataItem.Key,
			&dataItem.Version,
			&encodedContent,
			&dataItem.EventTime)
		if err != nil {
			return nil, err
		}
		var decodedContent JSON
		err = json.Unmarshal(encodedContent, &decodedContent)
		if err != nil {
			return nil, err
		}
		dataItem.Content = decodedContent
		dataItems = append(dataItems, dataItem)
	}
	return dataItems, nil
}

// Store function
func (s postgresDataItemStore) Store(ns, key string, content JSON) (*DataItem, error) {
	encodedContent, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	sqlStatement := `
	INSERT INTO data_items (namespace, key, content)
	VALUES ($1, $2, $3)
	RETURNING
		id,
		namespace,
		key,
		version,
		content,
		timestamp_from_id(id) AS event_time`
	row := s.db.QueryRow(sqlStatement, ns, key, encodedContent)
	return rowToDataItem(row)
}

// HistoryOf function
func (s postgresDataItemStore) HistoryOf(ns, key string) ([]DataItem, error) {
	sqlStatement := `
	SELECT
		id,
		namespace,
		key,
		version,
		content,
		timestamp_from_id(id) AS event_time
	FROM data_items
	WHERE
		namespace = $1 AND
		key = $2`
	rows, err := s.db.Query(sqlStatement, ns, key)
	if err != nil {
		return nil, err
	}
	return rowsToDataItems(rows, 100)
}

// ListItems function
func (s postgresDataItemStore) ListItems(ns, key string, iterStartID int64, iterSize int) ([]DataItem, error) {
	if iterSize == 0 {
		iterSize = 100
	}
	var sqlStatement string
	var rows *sql.Rows
	var err error
	if iterStartID == 0 {
		sqlStatement = `
		SELECT
			id,
			namespace,
			key,
			version,
			content,
			timestamp_from_id(id) AS event_time
		FROM data_items
		WHERE
			namespace = $1 AND
			key = $2
		ORDER BY id DESC
		LIMIT $3`
		rows, err = s.db.Query(sqlStatement, ns, key, iterSize)
	} else {
		sqlStatement = `
		SELECT
			id,
			namespace,
			key,
			version,
			content,
			timestamp_from_id(id) AS event_time
		FROM data_items
		WHERE
			namespace = $1 AND
			key = $2 AND
			id < $3
		ORDER BY id DESC
		LIMIT $4`
		rows, err = s.db.Query(sqlStatement, ns, key, iterStartID, iterSize)
	}
	if err != nil {
		return nil, err
	}
	return rowsToDataItems(rows, iterSize)
}

// ListByAuthor function
func (s postgresDataItemStore) ListByAuthor(ns, key, author string, iterStartID int64, iterSize int) ([]DataItem, error) {
	if iterSize == 0 {
		iterSize = 100
	}
	var sqlStatement string
	var rows *sql.Rows
	var err error
	if iterStartID == 0 {
		sqlStatement = `
		SELECT
			id,
			namespace,
			key,
			version,
			content,
			timestamp_from_id(id) AS event_time
		FROM data_items
		WHERE
			namespace = $1 AND
			key = $2 AND
			content->>'author' = $3
		ORDER BY id DESC
		LIMIT $4`
		rows, err = s.db.Query(sqlStatement, ns, key, author, iterSize)
	} else {
		sqlStatement = `
		SELECT
			id,
			namespace,
			key,
			version,
			content,
			timestamp_from_id(id) AS event_time
		FROM data_items
		WHERE
			namespace = $1 AND
			key = $2 AND
			content->>'author' = $3 AND
			id < $4
		ORDER BY id DESC
		LIMIT $5`
		rows, err = s.db.Query(sqlStatement, ns, key, author, iterStartID, iterSize)
	}
	if err != nil {
		return nil, err
	}
	return rowsToDataItems(rows, iterSize)
}

// ListNamespaces function
func (s postgresDataItemStore) ListNamespaces(iterStartID int64, iterSize int) ([]NamespaceMostRecentID, error) {
	if iterSize == 0 {
		iterSize = 100
	}
	var sqlStatement string
	var rows *sql.Rows
	var err error
	if iterStartID == 0 {
		sqlStatement = `
		SELECT
			namespace,
			most_recent_id
		FROM namespace_most_recent_ids
		ORDER BY most_recent_id DESC
		LIMIT $1`
		rows, err = s.db.Query(sqlStatement, iterSize)
	} else {
		sqlStatement = `
		SELECT
			namespace,
			most_recent_id
		FROM namespace_most_recent_ids
		WHERE most_recent_id < $1
		ORDER BY most_recent_id DESC
		LIMIT $2`
		rows, err = s.db.Query(sqlStatement, iterStartID, iterSize)
	}
	if err != nil {
		return nil, err
	}
	namespaceMRIDs := make([]NamespaceMostRecentID, 0, iterSize)
	for rows.Next() {
		var namespaceMRID NamespaceMostRecentID
		err := rows.Scan(
			&namespaceMRID.Namespace,
			&namespaceMRID.MostRecentID)
		if err != nil {
			return nil, err
		}
		namespaceMRIDs = append(namespaceMRIDs, namespaceMRID)
	}
	return namespaceMRIDs, nil
}

func main() {
	connStr := "dbname=postgres user=postgres host=localhost port=5533 sslmode=disable"
	store, err := NewDataItemStore(connStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Store")
	dataItem, err := store.Store("n", "k", JSON{"author": "a", "foo": "bar"})
	fmt.Println(dataItem, err)
	dataItem, err = store.Store("n", "k", JSON{"author": "a", "foo": "baz"})
	fmt.Println(dataItem, err)
	fmt.Println("HistoryOf")
	dataItems, err := store.HistoryOf("n", "k")
	fmt.Println(dataItems, err)
	fmt.Println("ListItems")
	dataItems, err = store.ListItems("n", "k", 0, 1)
	fmt.Println(dataItems, err)
	dataItems, err = store.ListItems("n", "k", dataItems[len(dataItems)-1].ID, 1)
	fmt.Println(dataItems, err)
	fmt.Println("ListByAuthor")
	dataItems, err = store.ListByAuthor("n", "k", "a", 0, 1)
	fmt.Println(dataItems, err)
	dataItems, err = store.ListByAuthor("n", "k", "a", dataItems[len(dataItems)-1].ID, 1)
	fmt.Println(dataItems, err)
	fmt.Println("ListNamespaces")
	namespaceMRIDs, err := store.ListNamespaces(0, 5)
	fmt.Println(namespaceMRIDs, err)
	namespaceMRIDs, err = store.ListNamespaces(namespaceMRIDs[len(namespaceMRIDs)-1].MostRecentID, 5)
	fmt.Println(namespaceMRIDs, err)
}
