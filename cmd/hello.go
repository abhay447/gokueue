package main

import (
	"fmt"

	logWriter "gokueue/internal/writer"
)

func main() {
	fmt.Println("hello world")
	logWriter.InitLogWriterMap()
	topicName := "test"
	msg := "\"hello\""
	writer, err := logWriter.GetWriter(topicName)
	if err != nil {
		fmt.Println(err)
		writer = logWriter.CreateTopic(topicName)
	}
	logWriter.WriteLogs(&writer, msg)
	logWriter.WriteLogs(&writer, msg)
	logWriter.WriteLogs(&writer, msg)
}
