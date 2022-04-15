package ports

import "download/types"

type Identifiable[T types.Identifier] interface {
	Identifier() T
}

type ICursor[T types.Identifier] interface {
	Start() T
	End() T
}
