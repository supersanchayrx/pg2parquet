package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const fileName = "_checkpoint.json"

type Checkpoint struct {
	CommitLSN uint64 `json:"commit_lsn"`
	EndLSN    uint64 `json:"end_lsn"`
}

func Load(dir string) (Checkpoint, error) {
	var cp Checkpoint
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if os.IsNotExist(err) {
		return cp, nil
	}
	if err != nil {
		return cp, fmt.Errorf("checkpoint: read: %w", err)
	}
	if err := json.Unmarshal(data, &cp); err != nil {
		return cp, fmt.Errorf("checkpoint: parse: %w", err)
	}
	return cp, nil
}

func Save(dir string, cp Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, fileName+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("checkpoint: create: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("checkpoint: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("checkpoint: close: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, fileName)); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	SyncDir(dir)
	return nil
}

func FileName(first, last uint64) string {
	return fmt.Sprintf("%016X_%016X.parquet", first, last)
}

func ParseFileName(name string) (uint64, uint64, bool) {
	name = strings.TrimSuffix(name, ".parquet")
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseUint(parts[0], 16, 64)
	last, err2 := strconv.ParseUint(parts[1], 16, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return first, last, true
}

func Cleanup(dir string, cp Checkpoint) (int, error) {
	removed := 0
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		remove := false
		if strings.HasSuffix(info.Name(), ".tmp") {
			remove = true
		} else if strings.HasSuffix(info.Name(), ".parquet") {
			_, last, ok := ParseFileName(info.Name())
			if ok && last > cp.CommitLSN {
				remove = true
			}
		}
		if remove {
			if err := os.Remove(path); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, fmt.Errorf("checkpoint: cleanup: %w", err)
	}
	return removed, nil
}

func SyncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	d.Sync()
	d.Close()
}
