package utils

func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
