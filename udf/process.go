package udf

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
)

var ErrProcessAborted = errors.New("process aborted")

type ByteReadReader interface {
	io.Reader
	io.ByteReader
}

type ByteReadCloser interface {
	io.ReadCloser
	io.ByteReader
}

type byteReadCloser struct {
	*bufio.Reader
	io.Closer
}

type Command interface {
	Start() error
	Wait() error

	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)
}

func NewCommand(prog string, args ...string) Command {
	return exec.Command(prog, args...)
}

// Wraps an external process and sends and receives data
// over STDIN and STDOUT. Lines received over STDERR are logged
// via normal Kapacitor logging.
//
// Once a Process is created and started the owner can send points or batches
// to the subprocess by writing them to the PointIn or BatchIn channels respectively,
// and according to the type of process created.
//
// The Process may be Aborted at anytime for various reasons, it is the owners responsibility
// via the AbortCallback to stop writing to the *In channels since no more selects on the channels
// will be performed.
//
// Calling Stop on the process should only be done once the owner has closed the *In channel,
// at which point the remaining data will be processed and the subprocess will exit cleanly.
type Process struct {
	// If the processes is Aborted (via KeepAlive timeout, etc.)
	// then no more data will be read off the *In channels.
	//
	// Optional callback if the process aborts.
	// It is the owners response
	abortCallback func()

	pointIn chan models.Point
	PointIn chan<- models.Point
	batchIn chan models.Batch
	BatchIn chan<- models.Batch

	pointOut chan models.Point
	PointOut <-chan models.Point
	batchOut chan models.Batch
	BatchOut <-chan models.Batch

	closing  chan struct{}
	aborting chan struct{}

	keepalives       chan *Request
	keepalive        chan int64
	keepaliveTimeout time.Duration

	cmd    Command
	stdin  io.WriteCloser
	stdout ByteReadCloser
	stderr io.ReadCloser

	readErr  chan error
	writeErr chan error
	err      error

	mu     sync.Mutex
	logger *log.Logger
	wg     sync.WaitGroup

	responseBuf []byte
	response    *Response

	batch *models.Batch
}

func NewProcess(cmd Command, wants, provides pipeline.EdgeType, l *log.Logger, timeout time.Duration, abortCallback func()) *Process {
	p := &Process{
		cmd:              cmd,
		logger:           l,
		keepalives:       make(chan *Request),
		keepalive:        make(chan int64, 1),
		keepaliveTimeout: timeout,
		abortCallback:    abortCallback,
	}
	switch wants {
	case pipeline.StreamEdge:
		p.pointIn = make(chan models.Point)
		p.PointIn = p.pointIn
	case pipeline.BatchEdge:
		p.batchIn = make(chan models.Batch)
		p.BatchIn = p.batchIn
	}
	switch provides {
	case pipeline.StreamEdge:
		p.pointOut = make(chan models.Point)
		p.PointOut = p.pointOut
	case pipeline.BatchEdge:
		p.batchOut = make(chan models.Batch)
		p.BatchOut = p.batchOut
	}
	return p
}

// Start the Process
func (p *Process) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closing = make(chan struct{})
	p.aborting = make(chan struct{})
	p.writeErr = make(chan error, 1)
	p.readErr = make(chan error, 1)

	stdin, err := p.cmd.StdinPipe()
	if err != nil {
		return err
	}
	p.stdin = stdin

	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	brc := byteReadCloser{
		bufio.NewReader(stdout),
		stdout,
	}
	p.stdout = brc

	stderr, err := p.cmd.StderrPipe()
	if err != nil {
		return err
	}
	p.stderr = stderr

	err = p.cmd.Start()
	if err != nil {
		return err
	}

	err = p.initProcess()
	if err != nil {
		return err
	}

	go func() {
		p.writeErr <- p.writeData()
	}()
	go func() {
		p.readErr <- p.readData()
	}()

	p.wg.Add(3)
	go p.runKeepAlive()
	go p.watchKeepAlive()
	go p.logStdErr()

	return nil
}

// Abort the process.
// Data in-flight will not be processed.
func (p *Process) Abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.aborting)
	p.stop()
	if p.abortCallback != nil {
		p.abortCallback()
	}
}

// Stop the Process cleanly.
//
// Calling Stop should only be done once the owner has closed the *In channel,
// at which point the remaining data will be processed and the subprocess will exit cleanly.
func (p *Process) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stop()
}

// internal stop function you must acquire the lock before calling
func (p *Process) stop() error {

	close(p.closing)

	writeErr := <-p.writeErr
	readErr := <-p.readErr
	p.wg.Wait()

	if p.cmd != nil {
		p.cmd.Wait()
	}

	if writeErr != nil {
		return writeErr
	}
	return readErr
}

func (p *Process) initProcess() error {
	req := &Request{Message: &Request_Init{
		Init: &InitializeRequest{},
	}}
	p.writeRequest(req)
	return nil
}

func (p *Process) runKeepAlive() {
	defer p.wg.Done()
	defer close(p.keepalives)
	if p.keepaliveTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(p.keepaliveTimeout / 2)
	for {
		select {
		case <-ticker.C:
			req := &Request{Message: &Request_Keepalive{
				Keepalive: &KeepAliveRequest{
					Time: time.Now().UnixNano(),
				},
			}}
			p.keepalives <- req
		case <-p.closing:
			ticker.Stop()
			return
		}
	}
}
func (p *Process) watchKeepAlive() {
	// Defer functions are called LIFO.
	// We need to call p.Abort after p.wg.Done so we just set a flag
	aborted := false
	defer func() {
		if aborted {
			p.Abort()
		}
	}()
	defer p.wg.Done()
	if p.keepaliveTimeout <= 0 {
		return
	}
	last := time.Now().UnixNano()
	for {
		select {
		case last = <-p.keepalive:
		case <-time.After(p.keepaliveTimeout):
			p.logger.Println("E! keepalive timedout, last keepalive received was:", time.Unix(0, last))
			aborted = true
			return
		case <-p.closing:
			return
		}
	}
}

func (p *Process) writeData() error {
	defer p.stdin.Close()
	for {
		select {
		case pt, ok := <-p.pointIn:
			if ok {
				err := p.writePoint(pt)
				if err != nil {
					return err
				}
			} else {
				p.pointIn = nil
			}
		case bt, ok := <-p.batchIn:
			if ok {
				err := p.writeBatch(bt)
				if err != nil {
					return err
				}
			} else {
				p.batchIn = nil
			}
		case req, ok := <-p.keepalives:
			if ok {
				err := p.writeRequest(req)
				if err != nil {
					return err
				}
			} else {
				p.keepalives = nil
			}
		case <-p.aborting:
			return ErrProcessAborted
		}
		if p.pointIn == nil && p.batchIn == nil && p.keepalives == nil {
			break
		}
	}
	return nil
}

func (p *Process) writePoint(pt models.Point) error {
	strs, floats, ints := p.fieldsToTypedMaps(pt.Fields)
	udfPoint := &Point{
		Time:            pt.Time.UnixNano(),
		Name:            pt.Name,
		Database:        pt.Database,
		RetentionPolicy: pt.RetentionPolicy,
		Group:           string(pt.Group),
		Dimensions:      pt.Dimensions,
		Tags:            pt.Tags,
		FieldsDouble:    floats,
		FieldsInt:       ints,
		FieldsString:    strs,
	}
	req := &Request{}
	req.Message = &Request_Point{udfPoint}
	return p.writeRequest(req)
}

func (p *Process) fieldsToTypedMaps(fields models.Fields) (
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
) {
	for k, v := range fields {
		switch value := v.(type) {
		case string:
			if strs == nil {
				strs = make(map[string]string)
			}
			strs[k] = value
		case float64:
			if floats == nil {
				floats = make(map[string]float64)
			}
			floats[k] = value
		case int64:
			if ints == nil {
				ints = make(map[string]int64)
			}
			ints[k] = value
		default:
			panic("unsupported field value type")
		}
	}
	return
}

func (p *Process) typeMapsToFields(
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
) models.Fields {
	fields := make(models.Fields)
	for k, v := range strs {
		fields[k] = v
	}
	for k, v := range ints {
		fields[k] = v
	}
	for k, v := range floats {
		fields[k] = v
	}
	return fields
}

func (p *Process) writeBatch(b models.Batch) error {
	req := &Request{}
	req.Message = &Request_Begin{&BeginBatch{}}
	err := p.writeRequest(req)
	if err != nil {
		return err
	}
	rp := &Request_Point{}
	req.Message = rp
	for _, pt := range b.Points {
		strs, floats, ints := p.fieldsToTypedMaps(pt.Fields)
		udfPoint := &Point{
			Time:         pt.Time.UnixNano(),
			Group:        string(b.Group),
			Tags:         pt.Tags,
			FieldsDouble: floats,
			FieldsInt:    ints,
			FieldsString: strs,
		}
		rp.Point = udfPoint
		err := p.writeRequest(req)
		if err != nil {
			return err
		}
	}

	req.Message = &Request_End{
		&EndBatch{
			Name: b.Name,
		},
	}
	return p.writeRequest(req)
}

func (p *Process) writeRequest(req *Request) error {
	return WriteMessage(req, p.stdin)
}

func (p *Process) readData() error {
	defer p.stdout.Close()
	for {
		response, err := p.readResponse()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = p.handleResponse(response)
		if err != nil {
			return err
		}
	}
}

func (p *Process) readResponse() (*Response, error) {
	err := ReadMessage(&p.responseBuf, p.stdout, p.response)
	if err != nil {
		return nil, err
	}
	return p.response, nil
}

func (p *Process) handleResponse(response *Response) error {
	// Always reset the keepalive timer since we received a response
	p.keepalive <- time.Now().UnixNano()
	switch msg := response.Message.(type) {
	case *Response_Keepalive:
		// Noop we already reset the keepalive timer
	case *Response_State:
	case *Response_Restore:
	case *Response_Error:
		p.logger.Println("E!", msg.Error.Error)
		return errors.New(msg.Error.Error)
	case *Response_Begin:
		p.batch = &models.Batch{}
	case *Response_Point:
		if p.batch != nil {
			pt := models.BatchPoint{
				Time: time.Unix(0, msg.Point.Time),
				Tags: msg.Point.Tags,
				Fields: p.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			p.batch.Points = append(p.batch.Points, pt)
		} else {
			pt := models.Point{
				Time:            time.Unix(0, msg.Point.Time),
				Name:            msg.Point.Name,
				Database:        msg.Point.Database,
				RetentionPolicy: msg.Point.RetentionPolicy,
				Group:           models.GroupID(msg.Point.Group),
				Dimensions:      msg.Point.Dimensions,
				Tags:            msg.Point.Tags,
				Fields: p.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			p.pointOut <- pt
		}
	case *Response_End:
		p.batch.Name = msg.End.Name
		p.batch.TMax = time.Unix(0, msg.End.TMax)
		p.batch.Group = models.GroupID(msg.End.Group)
		p.batch.Tags = msg.End.Tags
		p.batchOut <- *p.batch
		p.batch = nil
	default:
		panic(fmt.Sprintf("unexpected response message %T", msg))
	}
	return nil
}

func (p *Process) logStdErr() {
	defer p.wg.Done()
	defer p.stderr.Close()
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		p.logger.Println("E!", scanner.Text())
	}
}

// Write the message to the io.Writer with a varint size header.
func WriteMessage(msg proto.Message, w io.Writer) error {
	// marshal message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	varint := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(varint, uint64(len(data)))

	w.Write(varint[:n])
	w.Write(data)
	return nil
}

// Read a message from io.ByteReader by first reading a varint size,
// and then reading and decoding the message object.
// If buf is not big enough a new buffer will be allocated to replace buf.
func ReadMessage(buf *[]byte, r ByteReadReader, msg proto.Message) error {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if cap(*buf) < int(size) {
		*buf = make([]byte, size)
	}
	b := (*buf)[:size]
	n, err := r.Read(b)
	if err == io.EOF {
		return fmt.Errorf("unexpected EOF, expected %d more bytes", size)
	}
	if err != nil {
		return err
	}
	if n != int(size) {
		return fmt.Errorf("unexpected EOF, expected %d more bytes", int(size)-n)
	}
	err = proto.Unmarshal(b, msg)
	if err != nil {
		return err
	}
	return nil
}
