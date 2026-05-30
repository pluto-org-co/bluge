package pool

// Reduces memory fragmentation by creating data in larger pages
type Pool[T any] struct {
	data     []T
	index    uint8
	pageSize uint8
}

func (p *Pool[T]) Get() (v *T) {
	v = &p.data[p.index]
	if p.index+1 == p.pageSize {
		p.index = 0
		p.data = make([]T, p.pageSize)
	} else {
		p.index++
	}
	return v
}

func New[T any](pageSize uint8) (p *Pool[T]) {
	if pageSize == 0xFF {
		panic("255 is an invalid page size")
	}
	p = &Pool[T]{
		index:    0,
		pageSize: pageSize,
		data:     make([]T, pageSize),
	}
	return p
}
