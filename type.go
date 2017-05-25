package setcd

type Kind int

const (
	Invalid Kind = iota
	Nil
	Scale
	Slice
	Map
)

type SKind string

const (
	SInvalid SKind = "invalid"
	SNil           = "nil"
	SScale         = "scale"
	SSlice         = "slice"
	SMap           = "map"
)

func (k Kind) String() string {
	return string(k.ConvSKind())
}
func (k Kind) ConvSKind() SKind {
	switch k {
	case Nil:
		return SNil
	case Scale:
		return SScale
	case Slice:
		return SSlice
	case Map:
		return SMap
	case Invalid:
		return SInvalid
	default:
		return SInvalid
	}
}

func (s SKind) ConvKind() Kind {
	switch s {
	case SNil:
		return Nil
	case SScale:
		return Scale
	case SSlice:
		return Slice
	case SMap:
		return Map
	case SInvalid:
		return Invalid
	default:
		return Invalid
	}
}
