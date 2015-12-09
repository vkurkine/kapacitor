package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/gogo/protobuf/proto"

	"github.com/influxdb/kapacitor/udf"
)

var writeLock sync.Mutex

func main() {

	cmd := exec.Command("python2", "server.py")
	inP, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	outP, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	errP, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	cmd.Start()

	// Read protobufs off stdout
	done := make(chan error, 1)
	go func() {
		defer outP.Close()
		out := bufio.NewReader(outP)
		buf := make([]byte, 1024)
		response := &udf.Response{}
		for {
			size, err := binary.ReadUvarint(out)
			if err != nil {
				done <- err
				return
			}
			log.Println("Read size", size)
			if cap(buf) < int(size) {
				buf = make([]byte, size)
			}
			n, err := out.Read(buf[:size])
			if err == io.EOF {
				break
			}
			if err != nil {
				done <- err
				return
			}
			if n != int(size) {
				done <- fmt.Errorf("missing %d bytes", int(size)-n)
				return
			}
			proto.Unmarshal(buf, response)
			log.Println(response.Message)

		}
		done <- nil
	}()

	// Read stderr of process and echo to stderr
	doneErr := make(chan error, 1)
	go func() {
		defer errP.Close()
		buf := make([]byte, 1024)
		for {
			n, err := errP.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				doneErr <- err
				return
			}
			os.Stderr.Write(buf[:n])
		}
		doneErr <- nil
	}()

	// Write points over stdin
	donePoints := make(chan error, 1)
	go func() {
		p := &udf.Point{
			Name:      "test_proto",
			FieldsInt: make(map[string]int64, 0),
		}
		req := &udf.Request{}
		req.Message = &udf.Request_Point{p}
		for i := int64(0); i < 1e1; i++ {
			p.FieldsInt["value"] = i
			err := writeRequest(inP, req)
			if err != nil {
				donePoints <- err
				return
			}
		}
		donePoints <- nil
	}()

	// Write some request messages over stdin
	req := &udf.Request{}
	req.Message = &udf.Request_Restore{
		&udf.RestoreRequest{
			Version:  0,
			Snapshot: make([]byte, 128),
		},
	}
	err = writeRequest(inP, req)
	if err != nil {
		log.Fatal(err)
	}

	req.Message = &udf.Request_Keepalive{
		&udf.KeepAliveRequest{
			Time: 42,
		},
	}
	err = writeRequest(inP, req)
	if err != nil {
		log.Fatal(err)
	}
	err = writeRequest(inP, req)
	if err != nil {
		log.Fatal(err)
	}
	err = writeRequest(inP, req)
	if err != nil {
		log.Fatal(err)
	}
	err = writeRequest(inP, req)
	if err != nil {
		log.Fatal(err)
	}

	err = <-donePoints
	inP.Close()
	if err != nil {
		log.Println("done", err)
	}

	// Wait for any error
	select {
	case err := <-done:
		log.Println("done", err)
	case err := <-doneErr:
		log.Println("doneErr", err)
	}
}

// Write a protobuf request
func writeRequest(in io.Writer, req *udf.Request) error {
	writeLock.Lock()
	defer writeLock.Unlock()
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	varint := make([]byte, 10)
	n := binary.PutUvarint(varint, uint64(len(data)))
	in.Write(varint[:n])
	in.Write(data)
	return nil
}
