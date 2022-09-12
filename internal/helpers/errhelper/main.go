package errhelper

import "log"

func FailOnError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
