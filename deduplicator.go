package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type deduplicator struct {
	ch chan bool

	ready bool

	reportEvery time.Duration

	address string
	ln      *net.TCPListener

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	output  io.StringWriter
	numbers map[string]bool

	newUnique     uint32
	newDuplicated uint32
}

func newDeduplicator(address string, maxClients int, output io.StringWriter,
	reportEvery time.Duration) (*deduplicator, error) {
	if maxClients < 1 {
		return nil, fmt.Errorf("got maxClients `%d`: must be equal to 1 or greater", maxClients)
	}

	d := &deduplicator{
		ch:          make(chan bool, maxClients),
		reportEvery: reportEvery,
		address:     address,
		output:      output,
		numbers:     make(map[string]bool),
	}

	d.ctx, d.cancel = context.WithCancel(context.Background())

	for i := 0; i < cap(d.ch); i++ {
		d.ch <- true
	}

	return d, nil
}

func (d *deduplicator) start() error {
	if closer, implements := d.output.(io.Closer); implements {
		defer func(output io.Closer) {
			if err := output.Close(); err != nil {
				log.Printf("failed to close output file: %s", err)
			}
		}(closer)
	}

	addr, err := net.ResolveTCPAddr("tcp", d.address)
	if err != nil {
		return fmt.Errorf("failed to resolve tcp address `%s`: %w", d.address, err)
	}

	d.ln, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to create a tcp listener in address `%s`: %w", addr.String(), err)
	}
	defer func() {
		if err := d.ln.Close(); err != nil {
			log.Printf("failed to close TCP server: %s", err)
		}
	}()

	d.ready = true
	defer func() {
		d.ready = false
	}()

	log.Printf("TCP server listening in address %s", d.ln.Addr().String())
	log.Printf("max concurrent clients set to %d", cap(d.ch))

	var wg sync.WaitGroup

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		ticker := time.NewTicker(d.reportEvery)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				newUnique := atomic.SwapUint32(&d.newUnique, uint32(0))
				newDuplicated := atomic.SwapUint32(&d.newDuplicated, uint32(0))

				log.Printf("Received %d unique numbers, %d duplicates. Unique total: %d",
					newUnique, newDuplicated, len(d.numbers))
			case <-d.ctx.Done():
				return
			}
		}
	}(&wg)

Outer:
	for {
		select {
		case <-d.ctx.Done():
			break Outer
		case <-d.ch:
			if err := d.ln.SetDeadline(time.Now().Add(time.Second)); err != nil {
				log.Printf("failed to set deadline: %s", err)
				d.ch <- true
				continue Outer
			}

			conn, err := d.ln.Accept()
			if err != nil {
				if !isTimeoutErr(err) {
					log.Printf("failed to accept tcp connection: %s", err)
				}
				d.ch <- true
				continue Outer
			}

			log.Print("a client has been connected")

			wg.Add(1)
			go d.handleTCPConnection(&wg, conn)
		default:
			if err := d.ln.SetDeadline(time.Now().Add(time.Second)); err != nil {
				log.Printf("failed to set deadline: %s", err)
				continue Outer
			}

			conn, err := d.ln.Accept()
			if err != nil {
				if !isTimeoutErr(err) {
					log.Printf("failed to accept tcp connection: %s", err)
				}
				continue Outer
			}

			log.Print("maximum number of concurrent clients reached. disconnecting client")

			conn.Close()
		}
	}

	log.Print("shutting down deduplicator ...")

	wg.Wait()

	log.Print("deduplicator shutted down!")

	return nil
}

func (d *deduplicator) handleTCPConnection(wg *sync.WaitGroup, conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close client's connection: %s", err)
		}
		d.ch <- true
		wg.Done()
	}()

	reader := bufio.NewReader(conn)

Outer:
	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			err := conn.SetReadDeadline(time.Now().Add(time.Second * 3))
			if err != nil {
				log.Printf("failed to set read deadline: %s", err)

				if !isTemporaryErr(err) {
					return
				}

				continue Outer
			}

			str, err := reader.ReadString(byte('\n'))
			if err != nil {
				switch {
				case isTimeoutErr(err):
					continue Outer
				case err == io.EOF, !isTemporaryErr(err):
					log.Printf("client error: %s", err)
					return
				default:
					continue Outer
				}

			}

			if len(str) != 10 {
				// Invalid input: discard it and terminate connection.
				return
			}

			if str == "terminate\n" {
				d.cancel()
				return
			}

			_, err = strconv.Atoi(str[:8])
			if err != nil {
				// Invalid input: neither a 9 digits number or a `terminate`.
				// Terminate client connection without any report.
				return
			}

			d.mu.Lock()

			_, exists := d.numbers[str]
			if exists {
				_ = atomic.AddUint32(&d.newDuplicated, uint32(1))
				d.mu.Unlock()
				continue Outer
			}

			_ = atomic.AddUint32(&d.newUnique, uint32(1))
			d.numbers[str] = true
			d.output.WriteString(str)

			d.mu.Unlock()
		}
	}
}

func isTimeoutErr(err error) bool {
	if err, ok := err.(net.Error); ok {
		return err.Timeout()
	}
	return false
}

func isTemporaryErr(err error) bool {
	if err, ok := err.(net.Error); ok {
		return err.Temporary()
	}
	return true
}
