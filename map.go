package setcd

import (
	"fmt"
	"reflect"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

// (c *Client) GetMap ...
func (c *Client) GetMap(oos ...OpOption) (map[string]interface{}, error) {
	opt := parseOption(oos)

	// check type
	kind, err := c.mdGetKind()
	if err != nil {
		return nil, err
	}
	if kind != Map {
		return nil, fmt.Errorf("invalid map type on '%s'", c.odir)
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

	ret, err := c.kvParseMap(resp.Kvs)

	if opt.eval {
		return c.evalMap(ret, opt.evalTags, opt.evalVarFmt, opt.evalVarCheck)
	}

	return ret, nil
}

func (c *Client) evalMap(val map[string]interface{}, etags map[string]string,
	varFmt func(string) string, varCheck func(string) error) (map[string]interface{}, error) {

	ev, err := c.eval(val, etags, varFmt, varCheck)
	if err != nil {
		return nil, err
	}

	return ev.(map[string]interface{}), nil
}

func (c *Client) PutMap(in map[string]interface{}) error {
	_, err := concurrency.NewSTM(c.Client, func(stm concurrency.STM) error {
		s := newSTM(stm, c)
		return s.putMap(in)
	}, concurrency.WithAbortContext(c.ctx))

	return err
}

func (s *STM) putMap(in interface{}) error {
	v := reflect.ValueOf(in)
	if v.Kind() != reflect.Map {
		return fmt.Errorf("required map type of STM.putMap")
	}
	if s.mdGetKind() != Nil && s.mdGetKind() != Map {
		return fmt.Errorf("invalid map type on '%s'", s.odir)
	}

	oldLen, err := s.mdGetLen()
	if err != nil {
		return err
	}
	inLen := v.Len()
	newLen := oldLen + int64(inLen)
	for _, vkey := range v.MapKeys() {
		// put key list
		if vkey.Kind() != reflect.String {
			return fmt.Errorf("required string type of map")
		}
		key := vkey.String()
		s.mdPutIdx(key)
		ss, err := s.shadowClone(key, key)
		if err != nil {
			return err
		}
		vval := v.MapIndex(vkey)
		if err := ss.put(vval.Interface()); err != nil {
			return err
		}
	}
	s.mdPutKind(Map)
	s.mdPutLen(newLen)

	return nil
}

// DoMap ...
func (c *Client) DoMap(fn func(string, interface{}) bool, oos ...OpOption) error {
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

	if kind != Map {
		return fmt.Errorf("not Map type on '%s'", c.odir)
	}

	idxes, err := c.mdGetIdxes(etcdOpts...)
	if err != nil {
		return err
	}

	for _, key := range idxes {
		cs, err := c.shadowClone(key, key)
		if err != nil {
			return err
		}

		val, err := cs.Get(oos...)
		if err != nil {
			return err
		}

		if ok := fn(key, val); !ok {
			return nil
		}
	}
	return nil
}

// putMap ...
func (c *Client) putMap(in interface{}) (*clientv3.PutResponse, error) {
	v := reflect.ValueOf(in)
	if v.Kind() != reflect.Map {
		return nil, fmt.Errorf("required map type of Client.putMap, but is '%s'", v.Kind())
	}

	kind, err := c.mdGetKind()
	if err != nil {
		return nil, err
	}
	if kind != Nil && kind != Map {
		return nil, fmt.Errorf("invalid map type on '%s'", c.odir)
	}

	oldLen, err := c.mdGetLen()
	if err != nil {
		return nil, err
	}
	newLen := oldLen
	for _, vkey := range v.MapKeys() {
		// put key list
		if vkey.Kind() != reflect.String {
			return nil, fmt.Errorf("required string type of map")
		}
		key := vkey.String()
		isExists, err := c.mdIdxExists(key)
		if err != nil {
			return nil, err
		}
		if !isExists {
			_, err := c.mdPutIdx(key)
			if err != nil {
				return nil, err
			}
			newLen++
		}
		cs, err := c.shadowClone(key, key)
		if err != nil {
			return nil, err
		}
		vval := v.MapIndex(vkey)
		if _, err := cs.put(vval.Interface()); err != nil {
			return nil, err
		}
	}

	if kind == Nil {
		_, err = c.mdPutKind(Map)
		if err != nil {
			return nil, err
		}
	}
	return c.mdPutLen(newLen)
}
