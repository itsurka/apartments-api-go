package errhlp

import "log"

func Fatal(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
