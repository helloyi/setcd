package setcd

import (
	"fmt"
)

var (
	ErrInvalidArgument     = fmt.Errorf("invalid argument")
	ErrInvalidOperation    = fmt.Errorf("invalid operation")
	ErrUnsupportedOperaton = fmt.Errorf("unsupported operation")
	ErrUnknownType         = fmt.Errorf("unknown type")
	ErrUnsupportedType     = fmt.Errorf("unsupported type")

	ErrNotAbsoluteDir      = fmt.Errorf("%s: required an absolute directory", ErrInvalidArgument)
	ErrNotAllowedDir       = fmt.Errorf("%s: not allowed directory", ErrInvalidArgument)
	ErrEmptyDir            = fmt.Errorf("%s: empty dir", ErrInvalidOperation)
	ErrUnsupportedDeletion = fmt.Errorf("%s: 'Delete' dir on type", ErrUnsupportedOperaton)
	ErrUnsupportedDo       = fmt.Errorf("%s: 'Do' dir on type", ErrUnsupportedOperaton)
	ErrIndexOutOfRange     = fmt.Errorf("slice index out of range")
)
