package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"kvstore/internal/config"
	"kvstore/internal/metrics"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type WALRequest struct {
	Data []byte
	Done chan error
}

type WAL struct {
	cfg       *config.Config
	file      *os.File
	path      string
	queue     chan WALRequest
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// Entry Format: [keyLen:4][valLen:4][CRC32:4][key][value]

func OpenWAL(cfg *config.Config, dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal.log")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		cfg:       cfg,
		file:      f,
		path:      path,
		queue:     make(chan WALRequest, 10000),
		closeChan: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.groupCommitLoop()
	return w, nil
}

func (w *WAL) Append(key, value []byte) error {
	req := w.prepareRequest(key, value)
	w.queue <- req
	return <-req.Done
}

func (w *WAL) BatchAppend(keys, values [][]byte) error {
	var buf bytes.Buffer
	for i := range keys {
		data := append(keys[i], values[i]...)
		checksum := crc32.ChecksumIEEE(data)

		b := make([]byte, 12+len(keys[i])+len(values[i]))
		binary.LittleEndian.PutUint32(b[0:4], uint32(len(keys[i])))
		binary.LittleEndian.PutUint32(b[4:8], uint32(len(values[i])))
		binary.LittleEndian.PutUint32(b[8:12], checksum)
		copy(b[12:], keys[i])
		copy(b[12+len(keys[i]):], values[i])
		buf.Write(b)
	}
	req := WALRequest{Data: buf.Bytes(), Done: make(chan error, 1)}
	w.queue <- req
	return <-req.Done
}

func (w *WAL) prepareRequest(key, value []byte) WALRequest {
	data := append(key, value...)
	checksum := crc32.ChecksumIEEE(data)

	buf := make([]byte, 12+len(key)+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[8:12], checksum)
	copy(buf[12:], key)
	copy(buf[12+len(key):], value)
	return WALRequest{Data: buf, Done: make(chan error, 1)}
}

// Proper recovery with lastValidOffset tracking
func (w *WAL) Replay(apply func(k, v []byte)) error {
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	validEntries := 0
	corruptedEntries := 0
	lastValidOffset := int64(0)

	for {
		// Remember position before reading
		_, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			break
		}

		header := make([]byte, 12)
		if _, err := io.ReadFull(w.file, header); err != nil {
			if err == io.EOF {
				break
			}
			// Partial header - truncate here
			corruptedEntries++
			break
		}

		kLen := binary.LittleEndian.Uint32(header[0:4])
		vLen := binary.LittleEndian.Uint32(header[4:8])
		expectedChecksum := binary.LittleEndian.Uint32(header[8:12])

		// Sanity check
		if kLen > 10*1024*1024 || vLen > 100*1024*1024 {
			corruptedEntries++
			break
		}

		data := make([]byte, kLen+vLen)
		if _, err := io.ReadFull(w.file, data); err != nil {
			// Partial data - truncate here
			corruptedEntries++
			break
		}

		// Verify checksum
		actualChecksum := crc32.ChecksumIEEE(data)
		if actualChecksum != expectedChecksum {
			corruptedEntries++
			// Stop at corrupted entry
			break
		}

		// Valid entry
		apply(data[:kLen], data[kLen:])
		validEntries++

		// Track last valid offset AFTER successful read
		lastValidOffset, _ = w.file.Seek(0, io.SeekCurrent)
	}

	// Truncate to last valid offset
	if corruptedEntries > 0 {
		if err := w.file.Truncate(lastValidOffset); err != nil {
			return err
		}
		w.file.Seek(lastValidOffset, 0)
	}

	if corruptedEntries > 0 {
		// Log recovery stats
		// Don't return error, just log
		_ = corruptedEntries // Recovery successful, just skipped corrupted entries
	}

	return nil
}

func (w *WAL) groupCommitLoop() {
	defer w.wg.Done()
	var batch []WALRequest
	var writeBuf bytes.Buffer
	ticker := time.NewTicker(w.cfg.WALCommitInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		start := time.Now()

		_, err := w.file.Write(writeBuf.Bytes())
		if err == nil {
			err = w.file.Sync()
		}

		for _, req := range batch {
			req.Done <- err
		}

		if err == nil {
			metrics.GlobalMetrics.RecordWALSync(time.Since(start).Nanoseconds())
		}

		batch = batch[:0]
		writeBuf.Reset()
	}

	for {
		select {
		case req := <-w.queue:
			batch = append(batch, req)
			writeBuf.Write(req.Data)

			if len(batch) >= w.cfg.WALMaxBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-w.closeChan:
			flush()
			return
		}
	}
}

func (w *WAL) Close() error {
	close(w.closeChan)
	w.wg.Wait()
	return w.file.Close()
}
