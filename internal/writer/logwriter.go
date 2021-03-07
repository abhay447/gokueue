package logWriter

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/magiconair/properties"
)

type GkLogWriter struct {
	topicName string
	msgCount  int
}
type GkLogWriterMessasge struct {
	offset int
	data   string
}

var logWriterMap map[string]GkLogWriter = make(map[string]GkLogWriter)
var topicLogPath string

func InitLogWriterMap() {
	properties := properties.MustLoadFile("../config/config.properties", properties.UTF8)
	topicLogPath = properties.GetString("topic.log.path", "/var/log/gokueue")
	files, _ := ioutil.ReadDir(topicLogPath)
	for _, file := range files {
		fmt.Println(file.Name())
		logWriterMap[file.Name()] = GkLogWriter{file.Name(), 0}
	}
}

func loadLogWriter(topicName string) GkLogWriter {
	return logWriterMap[topicName]
}

func getFilePathForTopic(topicName string) string {
	currentTime := time.Now().UTC()
	filepath := topicLogPath + "/" + topicName + "/" +
		currentTime.Format("2006_01_02_15_") +
		fmt.Sprint(int(currentTime.Minute()/15)) +
		"_log.txt.gz"
	return filepath
}

func getCompressedMessage(logMsg GkLogWriterMessasge) bytes.Buffer {
	var compressedMsg bytes.Buffer
	gz := gzip.NewWriter(&compressedMsg)
	if _, err := gz.Write([]byte(fmt.Sprintf("%+v\n", logMsg))); err != nil {
		log.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		log.Fatal(err)
	}
	return compressedMsg
}

func CreateTopic(topicName string) GkLogWriter {
	err := os.MkdirAll(topicLogPath+"/"+topicName, 0755)
	if err != nil {
		println("error creating topic")
		fmt.Println(err)
	}
	logWriterMap[topicName] = GkLogWriter{
		topicName: topicName,
		msgCount:  0,
	}
	return logWriterMap[topicName]
}

func GetWriter(topicName string) (GkLogWriter, error) {
	if writer, ok := logWriterMap[topicName]; ok {
		//do something here
		return writer, nil
	}
	return GkLogWriter{}, fmt.Errorf("Topic not found, Please use create topic api")
}

func WriteLogs(writer *GkLogWriter, msg string) {
	filepath := getFilePathForTopic(writer.topicName)
	logMsg := GkLogWriterMessasge{writer.msgCount, msg}
	compressedMsg := getCompressedMessage(logMsg)
	fmt.Println(filepath)
	logFile, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		fmt.Println(err)
	}
	bufWriter := bufio.NewWriter(logFile)
	_, err = bufWriter.Write(compressedMsg.Bytes())
	if err != nil {
		fmt.Println(err)
	} else {
		writer.msgCount += 1
	}
	bufWriter.Flush()
}
