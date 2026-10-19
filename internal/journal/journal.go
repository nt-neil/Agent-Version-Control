// Package journal is a bounded write-ahead log of batch after-images.
//
// Each record holds the complete new content of every file a batch touched,
// so replay is idempotent: writing the same after-image twice is harmless.
// The log is truncated once those files are fsynced, so its size tracks
// in-flight work, never history.
package journal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type Record struct {
	Seq   uint64            `json:"seq"`
	Files map[string][]byte `json:"files"`
}

type Journal struct {
	f      *os.File
	size   int64
	broken error // set when a failed append could not be rolled back
}

// Open opens (or creates) the log and returns every intact record in order.
// A torn or corrupt tail is discarded.
func Open(path string) (*Journal, []Record, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	var recs []Record
	var off int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		sum := binary.LittleEndian.Uint32(hdr[4:])
		buf := make([]byte, n)
		if _, err := io.ReadFull(f, buf); err != nil || crc32.ChecksumIEEE(buf) != sum {
			break
		}
		var r Record
		if json.Unmarshal(buf, &r) != nil {
			break
		}
		recs = append(recs, r)
		off += 8 + int64(n)
	}
	if err := f.Truncate(off); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &Journal{f: f, size: off}, recs, nil
}

// Append writes one record and fsyncs. One call per batch: this is the group
// commit point every edit in the batch is acknowledged against. A failed
// append is cut back off the log so a later record never lands behind a torn
// one; if even that fails, the journal refuses all further writes.
func (j *Journal) Append(r Record) (err error) {
	if j.broken != nil {
		return j.broken
	}
	defer func() {
		if err == nil {
			return
		}
		if terr := j.rewind(j.size); terr != nil {
			j.broken = errors.New("journal: unrecoverable after failed append: " + terr.Error())
		}
	}()
	buf, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(buf) > 1<<31 {
		return errors.New("journal: record too large")
	}
	out := make([]byte, 8, 8+len(buf))
	binary.LittleEndian.PutUint32(out[:4], uint32(len(buf)))
	binary.LittleEndian.PutUint32(out[4:], crc32.ChecksumIEEE(buf))
	out = append(out, buf...)
	if _, err := j.f.Write(out); err != nil {
		return err
	}
	if err := j.f.Sync(); err != nil {
		return err
	}
	j.size += int64(len(out))
	return nil
}

func (j *Journal) rewind(off int64) error {
	if err := j.f.Truncate(off); err != nil {
		return err
	}
	if _, err := j.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	return j.f.Sync()
}

// Reset empties the log. Call only after every file it covers is durable.
func (j *Journal) Reset() error {
	if j.broken != nil {
		return j.broken
	}
	if err := j.rewind(0); err != nil {
		// The on-disk length is now unknown; refuse to append behind it.
		j.broken = errors.New("journal: reset failed: " + err.Error())
		return j.broken
	}
	j.size = 0
	return nil
}

func (j *Journal) Size() int64  { return j.size }
func (j *Journal) Close() error { return j.f.Close() }
