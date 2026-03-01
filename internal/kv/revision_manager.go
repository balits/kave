package kv

import (
	"errors"

	"github.com/balits/kave/internal/storage"
)

// Returned if we try to set the revision to a value lower than the current main counter
// As revisions are monotonically increasing, this is not allowed
var ErrLowerRevision error = errors.New("given revision is lower than current main counter")

type RevisionManager interface {
	// Current returns the current revision
	Current() Revision

	// UnsafeSet sets the main counter to the given revision's main counter and resets the sub counter to 0
	// This is unsafe and it will not compare the two revisions
	UnsafeSet(rev int64)

	// Merge merges the given revision into the main counter
	// This returns an error if the given revision is less than the current main counter
	CommitTransaction(rev Revision) (*Revision, error)

	// Called before every command execution
	// even singular SET and DELETE commands should happen inside a transaction
	// since they constitute one atomic execution, a 'revision'
	// A "transaction command", which itself has multiple subcommands
	// naturally should happen inside a transaction
	// If the transaction succeeds, it should be merged back to
	// the revision manager's main revision counter
	BeginTransaction() Revision
}

func NewRevisionManager(store *storage.Storage) RevisionManager {
	return &manager{
		main: 0,
		sub:  0,
	}
}

type manager struct {
	main int64
	sub  int64 // keeps track of ops inside a txn,  later we can use it for watches
}

func (m *manager) Current() Revision {
	return Revision{
		Main: m.main,
		Sub:  m.sub,
	}
}

func (m *manager) UnsafeSet(rev int64) {
	m.main = rev
	m.sub = 0
}

func (m *manager) CommitTransaction(rev Revision) (*Revision, error) {
	if rev.Main < m.main {
		return nil, ErrLowerRevision
	}

	if rev.Main == m.main {
		return nil, nil
	}

	m.main = rev.Main
	m.sub = 0

	return &Revision{
		Main: m.main,
		Sub:  m.sub,
	}, nil
}

func (m *manager) BeginTransaction() Revision {
	return Revision{
		Main: m.main + 1,
		Sub:  0,
	}
}
