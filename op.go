package setcd

import ()

type Option struct {
	tag      string // tag of a modify
	eval     bool   // evaluate a dir
	lock     bool   // put with lock
	keysOnly bool   // only get keys
	tagsOnly bool   // only get tags of first level dir

	evalTags     map[string]string
	evalVarFmt   func(string) string
	evalVarCheck func(string) error
}

type OpOption func(*Option)

func WithTag(tag string) OpOption {
	return func(op *Option) { op.tag = tag }
}

func WithEval() OpOption {
	return func(op *Option) { op.eval = true }
}

func WithLock() OpOption {
	return func(op *Option) { op.lock = true }
}

func WithKeysOnly() OpOption {
	return func(op *Option) { op.keysOnly = true }
}

func WithEvalTags(etags map[string]string) OpOption {
	return func(op *Option) { op.evalTags = etags }
}

func WithEvalVarFmt(f func(string) string) OpOption {
	return func(op *Option) { op.evalVarFmt = f }
}

func WithEvalVarCheck(f func(string) error) OpOption {
	return func(op *Option) { op.evalVarCheck = f }
}

func WithTagsOnly() OpOption {
	return func(op *Option) { op.tagsOnly = true }
}

func parseOption(oos []OpOption) *Option {
	opt := &Option{}
	for _, oo := range oos {
		oo(opt)
	}
	if opt.evalVarFmt == nil {
		opt.evalVarFmt = func(originVar string) string {
			return originVar
		}
	}
	if opt.evalVarCheck == nil {
		opt.evalVarCheck = func(tplVar string) error {
			return nil
		}
	}
	return opt
}
