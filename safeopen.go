// Package safeopen attempts to provide E[N,M]FILE safe alternatives to
// certain standard library os calls.
package safeopen

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
)

const defaultBackoffInterval = 100 * time.Millisecond

// BackoffCtor is a backoff implementation constructor.
type BackoffCtor func() backoff.BackOff

func newDefaultBackoff() backoff.BackOff {
	return backoff.NewConstantBackOff(defaultBackoffInterval)
}

// Opener is a "safe" opener instance.  The default value will use a per-call
// backoff of 100 ms.
type Opener struct {
	ctx      context.Context
	ctor     BackoffCtor
	notifier backoff.Notify
}

// Open is an E[N,M]FILE safe alternative to os.Open.
func (o *Opener) Open(name string) (*os.File, error) {
	return o.OpenFile(name, os.O_RDONLY, 0)
}

// Create is an E[N,M]FILE safe alternative to os.Create.
func (o *Opener) Create(name string) (*os.File, error) {
	return o.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// OpenFile is an E[N,M]FILE safe alternative to os.OpenFile.
func (o *Opener) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	// Use a new backoff instance per open attempt.
	ctx, ctor := o.ctx, o.ctor
	if ctx == nil {
		ctx = context.Background()
	}
	if ctor == nil {
		ctor = newDefaultBackoff
	}
	b := backoff.WithContext(ctor(), ctx)

	var f *os.File
	err := backoff.RetryNotify(func() error {
		var osErr error

		f, osErr = os.OpenFile(name, flag, perm)
		if osErr == nil {
			return nil
		}

		if isWrappedMNFile(osErr) {
			return osErr
		}

		return backoff.Permanent(osErr)
	}, b, o.notifier)

	return f, err
}

// WithContext sets the Opener's context for the purpose of cancelation.
func (o *Opener) WithContext(ctx context.Context) *Opener {
	o.ctx = ctx
	return o
}

// WithBackoffCtor sets the Opener's backoff implementation constructor.
func (o *Opener) WithBackoffCtor(ctor BackoffCtor) *Opener {
	o.ctor = ctor
	return o
}

// WithNotifier sets the Opener's retry notifier.
func (o *Opener) WithNotifier(notifier backoff.Notify) *Opener {
	o.notifier = notifier
	return o
}

// NewOpener creates a new Opener instance.
func NewOpener() *Opener {
	return &Opener{}
}

func isWrappedMNFile(err error) bool {
	if pErr, ok := err.(*os.PathError); ok {
		switch pErr.Err {
		case syscall.EMFILE, syscall.ENFILE:
			return true
		default:
		}
	}

	return false
}
