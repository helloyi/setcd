package setcd

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

// GetSlice ...
func (c *Client) GetSlice(oos ...OpOption) ([]interface{}, error) {
	opt := parseOption(oos)

	kind, err := c.mdGetKind()
	if err != nil {
		return nil, err
	}
	if kind != Slice {
		return nil, errors.New("not a slice type on " + c.odir)
	}

	etcdOpts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	if opt.tag != "" {
		rev, err := c.mdGetRev(opt.tag)
		if err != nil {
			return nil, err
		}
		etcdOpts = append(etcdOpts, clientv3.WithRev(rev))
	}

	resp, err := c.Client.Get(c.ctx, c.rdir, etcdOpts...)
	if err != nil {
		return nil, err
	}

	ret, err := c.kvParseSlice(resp.Kvs)

	if opt.eval {
		return c.evalSlice(ret, opt.evalTags, opt.evalVarFmt, opt.evalVarCheck)
	}

	return ret, err
}

func (c *Client) evalSlice(val []interface{}, etags map[string]string,
	varFmt func(string) string, varCheck func(string) error) ([]interface{}, error) {

	ev, err := c.eval(val, etags, varFmt, varCheck)
	if err != nil {
		return nil, err
	}

	return ev.([]interface{}), nil
}

//
// PUT
//

func (c *Client) PutSlice(in []interface{}, oos ...OpOption) error {
	opt := parseOption(oos)
	resp, err := concurrency.NewSTM(c.Client, func(stm concurrency.STM) error {
		s := newSTM(stm, c)
		return s.putSlice(in)
	}, concurrency.WithAbortContext(c.ctx))

	if err != nil {
		return err
	}

	if opt.tag != "" {
		rev := resp.Header.Revision
		return c.mdPutTag(opt.tag, rev)
	}

	return nil
}

// putSlice ...
func (s *STM) putSlice(sin interface{}) error {
	v := reflect.ValueOf(sin)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return fmt.Errorf("required slice/array type of STM.putSlice, but is '%s'", v.Kind())
	}
	// check type when update
	kind := s.mdGetKind()
	if kind != Nil && kind != Slice {
		return fmt.Errorf("invalid slice type on '%s', but is '%s'", s.odir, kind)
	}

	oldLen, err := s.mdGetLen()
	if err != nil {
		return err
	}
	id, err := s.mdGetLastID()
	if err != nil {
		return err
	}
	inLen := v.Len()
	newLen := oldLen + int64(inLen)

	for i := 0; i < inLen; i++ {
		// put idx table
		id += 1
		idx := fmt.Sprintf("%019d", id)
		s.mdPutIdx(idx)

		elem := v.Index(i)
		ds, err := s.shadowClone(strconv.FormatInt(int64(i)+oldLen, 10), idx)
		if err != nil {
			return err
		}
		if err := ds.put(elem.Interface()); err != nil {
			return err
		}
	}
	s.mdPutKind(Slice)
	s.mdPutLen(newLen)
	s.mdPutLastID(id)
	return nil
}

// DoSlice calls function fn on each element of the slice.
func (c *Client) DoSlice(fn func(int, interface{}) bool, oos ...OpOption) error {
	opt := parseOption(oos)

	var etcdOpts []clientv3.OpOption
	if opt.tag != "" {
		rev, err := c.mdGetRev(opt.tag)
		if err != nil {
			return err
		}
		etcdOpts = []clientv3.OpOption{clientv3.WithRev(rev)}
	}

	kind, err := c.mdGetKind(etcdOpts...)
	if err != nil {
		return err
	}

	if kind != Slice {
		return fmt.Errorf("not slice type on '%s'", c.odir)
	}

	idxes, err := c.mdGetIdxes(etcdOpts...)
	if err != nil {
		return err
	}

	for idx, key := range idxes {
		cs, err := c.shadowClone(strconv.Itoa(idx), key)
		if err != nil {
			return err
		}

		val, err := cs.Get(oos...)
		if err != nil {
			return err
		}

		if ok := fn(idx, val); !ok {
			return nil
		}
	}
	return nil
}

// putSlice ...
func (c *Client) putSlice(sin interface{}) (*clientv3.PutResponse, error) {
	v := reflect.ValueOf(sin)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("required slice/array type of Client.putSlice, but is '%s'", v.Kind())
	}
	// check type when update
	kind, err := c.mdGetKind()
	if err != nil {
		return nil, err
	}
	if kind != Nil && kind != Slice {
		return nil, fmt.Errorf("invalid slice type on '%s', but is '%s'", c.odir, kind)
	}

	oldLen, err := c.mdGetLen()
	if err != nil {
		return nil, err
	}
	id, err := c.mdGetLastID()
	if err != nil {
		return nil, err
	}
	inLen := v.Len()
	newLen := oldLen + int64(inLen)

	for i := 0; i < inLen; i++ {
		// put idx table
		id += 1
		idx := fmt.Sprintf("%019d", id)
		_, err := c.mdPutIdx(idx)
		if err != nil {
			return nil, err
		}

		fmt.Println("Client ", i, idx)
		elem := v.Index(i)
		ds, err := c.shadowClone(strconv.FormatInt(int64(i)+oldLen, 10), idx)
		if err != nil {
			return nil, err
		}
		if _, err := ds.put(elem.Interface()); err != nil {
			return nil, err
		}
	}
	_, err = c.mdPutKind(Slice)
	if err != nil {
		return nil, err
	}
	_, err = c.mdPutLen(newLen)
	if err != nil {
		return nil, err
	}
	return c.mdPutLastID(id)
}
