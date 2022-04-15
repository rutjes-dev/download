package model

import (
	"download/types"
)

type Cursor[T types.Identifier] struct {
	Start T
	End   T
}

func NewCursor[T types.Identifier](start, end T) *Cursor[T] {
	return &Cursor[T]{
		Start: start,
		End:   end,
	}
}
