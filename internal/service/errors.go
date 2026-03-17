package service

import "errors"

var (
	// Hiba amikor nem sikerül a raft.Apply any visszatérési értékét az adott tipusra castolni
	ErrUnexpectedResultType error = errors.New("unexpected result type")
)
