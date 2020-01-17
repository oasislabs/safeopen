package safeopen

import (
	"os"
	"syscall"
	"testing"
	"time"
)

const testFile = "/dev/null"

func TestSafeOpen(t *testing.T) {
	// Lower the rlimit to make this easier.
	maxFiles, err := lowerRlimit()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rlimit is now %d", maxFiles)

	// Try to consume all of the fds.
	dummyFds, err := consumeFds(maxFiles)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("consumed %d dummy fds", len(dummyFds))

	var retries int
	opener := NewOpener().WithNotifier(func(err error, delay time.Duration) {
		t.Logf("in opener notifier, closing a dummy fd")

		if retries != 0 {
			t.Fatalf("notifier called repeatedly???? %d", retries)
		}
		retries++

		_ = dummyFds[0].Close()
		dummyFds = dummyFds[1:]
	})
	fd, err := opener.Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	_ = fd.Close()
}

func lowerRlimit() (uint64, error) {
	const desiredLimit = 16

	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return 0, err
	}
	if desiredLimit >= rlim.Cur {
		return rlim.Cur, nil
	}
	rlim.Cur = desiredLimit
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return 0, err
	}

	return rlim.Cur, nil
}

func consumeFds(maxFiles uint64) ([]*os.File, error) {
	var (
		fds []*os.File
		ok  bool
	)
	defer func() {
		if !ok {
			for _, fd := range fds {
				_ = fd.Close()
			}
		}
	}()

	for i := uint64(0); i < maxFiles; i++ {
		fd, err := os.Open(testFile)
		if err != nil {
			if isWrappedMNFile(err) {
				break
			}
			return nil, err
		}
		fds = append(fds, fd)
	}

	ok = true

	return fds, nil
}
