package udf_test

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/udf"
)

type commandHelper struct {
	inr *io.PipeReader
	inw *io.PipeWriter

	outr *io.PipeReader
	outw *io.PipeWriter

	errr *io.PipeReader
	errw *io.PipeWriter

	requests chan *udf.Request
	errC     chan error
}

func newCommandHelper() *commandHelper {
	cmd := &commandHelper{
		requests: make(chan *udf.Request, 1),
		errC:     make(chan error, 1),
	}
	return cmd
}

func (c *commandHelper) run() error {
	defer c.inr.Close()
	defer close(c.requests)
	buf := bufio.NewReader(c.inr)
	var b []byte
	for {
		req := &udf.Request{}
		err := udf.ReadMessage(&b, buf, req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		c.requests <- req
	}
}

func (c *commandHelper) Start() error {
	go func() {
		c.errC <- c.run()
	}()
	return nil
}

func (c *commandHelper) Wait() error {
	return nil
}

// Wrapps the STDIN pipe and when it is closed
// closes the STDOUT and STDERR pipes of the command.
type cmdCloser struct {
	*io.PipeWriter
	cmd *commandHelper
}

func (cc *cmdCloser) Close() error {
	cc.cmd.outw.Close()
	cc.cmd.errw.Close()
	return cc.PipeWriter.Close()
}

func (c *commandHelper) StdinPipe() (io.WriteCloser, error) {
	c.inr, c.inw = io.Pipe()
	closer := &cmdCloser{
		PipeWriter: c.inw,
		cmd:        c,
	}
	return closer, nil
}

func (c *commandHelper) StdoutPipe() (io.ReadCloser, error) {
	c.outr, c.outw = io.Pipe()
	return c.outr, nil
}

func (c *commandHelper) StderrPipe() (io.ReadCloser, error) {
	c.errr, c.errw = io.Pipe()
	return c.errr, nil
}

func TestMessage_ReadWrite(t *testing.T) {
	req := &udf.Request{}
	req.Message = &udf.Request_Keepalive{
		Keepalive: &udf.KeepAliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	err := udf.WriteMessage(req, &buf)
	if err != nil {
		t.Fatal(err)
	}

	nreq := &udf.Request{}
	var b []byte
	err = udf.ReadMessage(&b, &buf, nreq)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(req, nreq) {
		t.Errorf("unexpected request: \ngot %v\nexp %v", nreq, req)
	}
}

func TestMessage_ReadWriteMultiple(t *testing.T) {
	req := &udf.Request{}
	req.Message = &udf.Request_Keepalive{
		Keepalive: &udf.KeepAliveRequest{
			Time: 42,
		},
	}

	var buf bytes.Buffer

	var count int = 1e4
	for i := 0; i < count; i++ {
		err := udf.WriteMessage(req, &buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	nreq := &udf.Request{}
	var b []byte

	for i := 0; i < count; i++ {
		err := udf.ReadMessage(&b, &buf, nreq)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(req, nreq) {
			t.Fatalf("unexpected request: i:%d \ngot %v\nexp %v", i, nreq, req)
		}
	}
}

func TestProcess_StartStop(t *testing.T) {
	cmd := newCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_Start] ", log.LstdFlags)
	p := udf.NewProcess(cmd, pipeline.StreamEdge, pipeline.StreamEdge, l, 0, nil)
	p.Start()
	req := <-cmd.requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}

	close(p.PointIn)
	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.requests {
	}
	if err := <-cmd.errC; err != nil {
		t.Error(err)
	}
}

func TestProcess_KeepAlive(t *testing.T) {
	cmd := newCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_KeepAlive] ", log.LstdFlags)
	p := udf.NewProcess(cmd, pipeline.StreamEdge, pipeline.StreamEdge, l, time.Millisecond*10, nil)
	p.Start()
	req := <-cmd.requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}
	req = <-cmd.requests
	_, ok = req.Message.(*udf.Request_Keepalive)
	if !ok {
		t.Error("expected keepalive message")
	}

	close(p.PointIn)
	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.requests {
	}
	if err := <-cmd.errC; err != nil {
		t.Error(err)
	}
}

func TestProcess_MissedKeepAlive(t *testing.T) {
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	cmd := newCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_MissedKeepAlive] ", log.LstdFlags)
	p := udf.NewProcess(cmd, pipeline.StreamEdge, pipeline.StreamEdge, l, 2, aborted)
	p.Start()
	req := <-cmd.requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}

	// Since the keepalive is missed, the process should abort on its own.
	for range cmd.requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Millisecond * 20):
		t.Error("expected abort callback to be called")
	}
	if err := <-cmd.errC; err != nil {
		t.Error(err)
	}
}
