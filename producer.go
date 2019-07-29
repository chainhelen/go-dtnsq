package nsq

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type producerConn interface {
	String() string
	SetLogger(logger, LogLevel, string)
	Connect() (*IdentifyResponse, error)
	Close() error
	WriteCommand(*Command) error
}

// Producer is a high-level type to publish to NSQ.
//
// A Producer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Producer struct {
	id     int64
	addr   string
	config Config

	// query nsqd instance for each topic assigned by lookupd
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int
	nsqdConnections map[string]*Conn // map topic to conn
	behaviorDelegate interface{}

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	responseChan     chan []byte
	incomingMessages chan *Message
	errorChan        chan []byte

	transactionChan chan *ProducerTransaction
	transactions    []*ProducerTransaction

	concurrentProducers int32
	stopFlag            int32
	exitChan            chan int
	wg                  sync.WaitGroup
	guard               sync.RWMutex

	checkBackHandler func(*Message) error
}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	topic    string
	cmd      *Command
	doneChan chan *ProducerTransaction
	Error    error         // the error (or nil) of the publish command
	Args     []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
	Response []byte
}

func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewProducer(addr string, config *Config) (*Producer, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	p := &Producer{
		id: atomic.AddInt64(&instCount, 1),

		addr:   addr,
		config: *config,

		nsqdConnections: make(map[string]*Conn),

		logger: log.New(os.Stderr, "", log.Flags()),
		logLvl: LogLevelInfo,

		transactionChan:  make(chan *ProducerTransaction),
		exitChan:         make(chan int),
		responseChan:     make(chan []byte),
		incomingMessages: make(chan *Message),
		errorChan:        make(chan []byte),
		// closeChan:        make(chan int),

	}
	p.wg.Add(1)
	go p.router()
	return p, nil
}

func (w *Producer) ConnectToNSQLookupd(addr string) error {
	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	w.guard.Lock()
	defer 	w.guard.Unlock()

	for _, x := range w.lookupdHTTPAddrs {
		if x == addr {
			return nil
		}
	}
	w.lookupdHTTPAddrs = append(w.lookupdHTTPAddrs, addr)
	return nil
}

func (w *Producer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := w.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Producer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	w.behaviorDelegate = cb
}

func (w *Producer) nextLookupdAddr() string {
	if w.lookupdQueryIndex >= len(w.lookupdHTTPAddrs) {
		w.lookupdQueryIndex = 0
	}
	addr := w.lookupdHTTPAddrs[w.lookupdQueryIndex]
	num := len(w.lookupdHTTPAddrs)
	w.lookupdQueryIndex = (w.lookupdQueryIndex + 1) % num
	return addr
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
func (w *Producer) Ping() error {
	err := w.connect("")
	if err != nil {
		return err
	}

	return w.nsqdConnections[""].WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (w *Producer) SetLogger(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logger = l
	w.logLvl = lvl
}

func (w *Producer) getLogger() (logger, LogLevel) {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logger, w.logLvl
}

// String returns the address of the Producer
func (w *Producer) String() string {
	return w.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
func (w *Producer) Stop() {
	w.guard.Lock()
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		w.guard.Unlock()
		return
	}
	w.log(LogLevelInfo, "stopping")
	close(w.exitChan)
	w.close()
	w.guard.Unlock()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(topic, Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(topic, cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) Publish(topic string, body []byte) error {
	return w.sendCommand(topic, Publish(topic, body))
}

// PublishDtPre synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) PublishDtPre(topic string, body []byte) ([]byte, error) {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(topic, PublishDtPre(topic, body), doneChan, nil)
	if err != nil {
		close(doneChan)
		return nil, err
	}
	t := <-doneChan
	return t.Response, t.Error
}

// PublishDtCmt synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) PublishDtCmt(topic string, body []byte) ([]byte, error) {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(topic, PublishDtCmt(topic, body), doneChan, nil)
	if err != nil {
		close(doneChan)
		return nil, err
	}
	t := <-doneChan
	return t.Response, t.Error
}

// PublishDtCnl synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (w *Producer) PublishDtCnl(topic string, body []byte) ([]byte, error) {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(topic, PublishDtCnl(topic, body), doneChan, nil)
	if err != nil {
		close(doneChan)
		return nil, err
	}
	t := <-doneChan
	return t.Response, t.Error
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (w *Producer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommand(topic, cmd)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
func (w *Producer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return w.sendCommand(topic, DeferredPublish(topic, delay, body))
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (w *Producer) DeferredPublishAsync(topic string, delay time.Duration, body []byte,
	doneChan chan *ProducerTransaction, args ...interface{}) error {
	return w.sendCommandAsync(topic, DeferredPublish(topic, delay, body), doneChan, args)
}

func (w *Producer) sendCommand(topic string, cmd *Command) error {
	doneChan := make(chan *ProducerTransaction)
	err := w.sendCommandAsync(topic, cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return err
	}
	t := <-doneChan
	return t.Error
}

func (w *Producer) sendCommandAsync(topic string, cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentProducers, 1)
	defer atomic.AddInt32(&w.concurrentProducers, -1)

	if err := w.connect(topic); err != nil {
		return err
	}

	t := &ProducerTransaction{
		topic:    topic,
		cmd:      cmd,
		doneChan: doneChan,
		Args:     args,
	}

	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}

	return nil
}

func (w *Producer) connect(topic string) error {
	w.guard.RLock()
	if _, ok := w.nsqdConnections[topic]; ok {
		w.guard.RUnlock()
		return nil
	}
	w.guard.RUnlock()

	w.guard.Lock()
	defer w.guard.Unlock()

	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	addr, err := w.getNSQD(topic)
	if err != nil {
		w.log(LogLevelError, "error getting nsqd addr for topic(%s) - %s", topic, err)
		return err
	}

	w.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	logger, logLvl := w.getLogger()

	conn := NewConn(addr, &w.config, &producerConnDelegate{w})
	conn.SetLogger(logger, logLvl, fmt.Sprintf("%3d (%%s)", w.id))
	_, err = conn.Connect()
	if err != nil {
		_ = conn.Close()
		w.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
		return err
	}

	w.nsqdConnections[topic] = conn

	return nil
}

func (w *Producer) getNSQD(topic string) (string, error) {
	if len(w.lookupdHTTPAddrs) == 0 {
		return w.addr, nil
	}

	lookupdAddr := w.nextLookupdAddr()
	var addresses []string
	var err error
	if topic != "" {
		endpoint := w.endpoint(lookupdAddr, "/lookup", map[string]string{"topic": topic})
		addresses, _ = w.queryLookupd(endpoint)
	}
	if len(addresses) == 0 {
		endpoint := w.endpoint(lookupdAddr, "/nodes", nil)
		addresses, err = w.queryLookupd(endpoint)
	}
	if err != nil {
		return "", err
	}
	if len(addresses) == 0 {
		return "", errors.New("no nsqd found")
	}
	return addresses[0], nil
}

func (w *Producer) endpoint(addr, path string, args map[string]string) string {
	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	u.Path = path

	if args != nil {
		val, _ := url.ParseQuery(u.RawQuery)
		for k, v := range args {
			val.Add(k, v)
		}
		u.RawQuery = val.Encode()
	}

	return u.String()
}

func (w *Producer) queryLookupd(endpoint string) ([]string, error) {
	w.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data lookupResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		w.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		return nil, err
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	// apply filter
	if discoveryFilter, ok := w.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}

	return nsqdAddrs, nil
}

func (w *Producer) closeConn(c *Conn) {
	w.guard.Lock()
	defer w.guard.Unlock()

	for topic, conn := range w.nsqdConnections {
		if c == conn {
			delete(w.nsqdConnections, topic)
			break
		}
	}
	_ = c.Close()
}

func (w *Producer) close() {
	for topic, conn := range w.nsqdConnections {
		delete(w.nsqdConnections, topic)
		_ = conn.Close()
	}
}

func (w *Producer) router() {
	for {
		select {
		case t := <-w.transactionChan:
			w.transactions = append(w.transactions, t)
			conn := w.nsqdConnections[t.topic]
			err := conn.WriteCommand(t.cmd)
			if err != nil {
				w.log(LogLevelError, "(%s) sending command - %s", conn.String(), err)
				w.closeConn(conn)
			}
		case data := <-w.responseChan:
			w.popTransaction(FrameTypeResponse, data)
		case msg := <-w.incomingMessages:
			w.HandleDtComingMessage(msg)
		case data := <-w.errorChan:
			w.popTransaction(FrameTypeError, data)
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	w.log(LogLevelInfo, "exiting router")
}

func (w *Producer) SetCheckBackHandler(cb func(*Message) error) {
	w.checkBackHandler = cb
}

func (w *Producer) HandleDtComingMessage(msg *Message) {
	if err := w.checkBackHandler(msg); err != nil {
		w.log(LogLevelError, "HandleDtComingMessage msg:%s, err:%s", msg, err)
		return
	}
	w.log(LogLevelDebug, "HandleDtComingMessage msg:%s", msg)
}

func (w *Producer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	w.transactions = w.transactions[1:]
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)}
	} else {
		t.Response = data
	}
	t.finish()
}

func (w *Producer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&w.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (w *Producer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := w.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, w.id, fmt.Sprintf(line, args...)))
}

func (w *Producer) onConnResponse(c *Conn, data []byte) { w.responseChan <- data }
func (w *Producer) onConnMessage(c *Conn, msg *Message) { w.incomingMessages <- msg }
func (w *Producer) onConnError(c *Conn, data []byte)    { w.errorChan <- data }
func (w *Producer) onConnHeartbeat(c *Conn)             {}
func (w *Producer) onConnIOError(c *Conn, err error)    { w.closeConn(c) }
func (w *Producer) onConnClose(c *Conn) { w.closeConn(c) }
