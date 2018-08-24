// 1/ CREATE KEYSPACE IF NOT EXISTS myCas WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor':1};
// 2/ CREATE TABLE user(id int PRIMARY KEY, user_name varchar, age int);
// 3/ CREATE INDEX ON KEYSPACE.TABLE (FIELD);

// docker run --name DB_NAME -d scylladb/scylla
// docker run --name DB_NAME --volume PATH/master_scylla.yaml:/etc/scylla/scylla.yaml -d scylladb/scylla
// index enable: docker run --name DB_NAME -d scylladb/scylla --experimental 1
// status: docker exec -it DB_NAME nodetool status
// cqlsh: docker exec -it DB_NAME cqlsh
// restart: docker exec -it DB_NAME supervisorctl restart scylla

// docker ps
// docker kill

package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster("172.17.0.1:9042")
	cluster.Keyspace = "myspace"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Failed CreateSession: %+v", err)
		return
	}
	defer session.Close()

	// create table
	err = CreateTable(session)
	if err != nil {
		log.Printf("Failed CreateTable: %+v", err)
		return
	}
	log.Printf("CreateTable: ok")

	// insert a user
	u := User{
		ID:   1,
		Name: "xin",
		Age:  29,
	}
	err = InsertUser(session, &u)
	if err != nil {
		log.Printf("Failed InsertUser: %+v", err)
		return
	}
	log.Printf("InsertUser: ok")

	// list
	err = Query(session)
	if err != nil {
		log.Printf("Failed Query: %+v", err)
		return
	}
	log.Printf("Query: ok")

}

func Query(session *gocql.Session) error {
	iter := session.Query(`SELECT id FROM user WHERE age > ?`, 0).Iter()
	var id int
	for iter.Scan(&id) {
		log.Printf("list id: %d", id)
	}
	return iter.Close()
}

func CreateTable(session *gocql.Session) error {
	query := fmt.Sprintf(`CREATE TABLE user(id int PRIMARY KEY, user_name varchar, age int);`)
	return session.Query(query).Exec()
}

func InsertUser(session *gocql.Session, u *User) error {
	return session.Query(`INSERT INTO user (id, user_name, age) VALUES (?, ?, ?)`,
		u.ID, u.Name, u.Age).Exec()
}

type User struct {
	ID   int
	Name string
	Age  int
}
