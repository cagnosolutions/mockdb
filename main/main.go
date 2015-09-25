package main

import (
	"fmt"
	"time"

	"github.com/cagnosolutions/mockdb"
)

type User struct {
	Id     int
	Name   string
	Email  string
	Active bool
}

func main() {

	db := mockdb.NewMockDB("backup.json", 5)

	/*
		id1 := db.Add("users", User{1, "Scott Cagno", "scottiecagno@gmail.com", true})
		fmt.Printf("Added user with id: %q\n", id1)

		id2 := db.Add("users", User{2, "Kayla Cagno", "kaylacagno@gmail.com", false})
		fmt.Printf("Added user with id: %q\n", id2)

		id3 := db.Add("users", User{3, "Greg Pechiro", "gregpechiro@gmail.com", true})
		fmt.Printf("Added user with id: %q\n", id3)

		id4 := db.Add("users", User{4, "Rosalie Pechiro", "rosaliepichero@gmail.com", false})
		fmt.Printf("Added user with id: %q\n", id4)

		id5 := db.Add("users", User{5, "Gabe Witmer", "gabewitmer@gmail.com", true})
		fmt.Printf("Added user with id: %q\n", id5)
	*/

	/*
		var user User
		id := "fcf06596-297a-46ca-a09f-e32b2fae6d59"
		db.GetAs("users", id, &user)
		fmt.Printf("Got user with id %q from db: %+v...\n", id, user)

		fmt.Printf("Modifying user...")
		user.Name = "Mario Mario"
		db.Set("users", id, user)
	*/

	var user User
	ok := db.Query("users", map[string]interface{}{"Id": 3, "Email": "gregpechiro@gmail.com"}, &user)
	if !ok {
		fmt.Println("Woops, couldn't find user!!!")
	} else {
		fmt.Println(user)
	}

	fmt.Println("Sleeping for 10 seconds...")
	time.Sleep(time.Duration(10) * time.Second)
}
