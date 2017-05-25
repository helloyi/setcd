package setcd

import (
	"errors"
	"strconv"
	"strings"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/helloyi/setcd/dir"
)

// kvParseKind ...
func (c *Client) kvParseKind(kvs []*mvccpb.KeyValue) (Kind, error) {
	if len(kvs) == 0 {
		return Nil, nil
	}

	key := string(kvs[0].Key)
	if c.rdir == key {
		return Scale, nil
	}

	// get kind from matedata: map || slice
	return c.mdGetKind()
}

// kvParse ...
func (c *Client) kvParse(kvs []*mvccpb.KeyValue) (interface{}, error) {
	kind, err := c.kvParseKind(kvs)
	if err != nil {
		return nil, err
	}

	switch kind {
	case Nil:
		return nil, nil
	case Scale:
		return c.kvParseScale(kvs)
	case Slice:
		return c.kvParseSlice(kvs)
	case Map:
		return c.kvParseMap(kvs)
	case Invalid:
		return nil, errors.New("Invalid type on " + c.odir)
		// return c.kvParseInvlid(kvs)
	default:
		return nil, errors.New("undefined type on " + c.odir)
	}
}

// kvParseScale ...
func (c *Client) kvParseScale(kvs []*mvccpb.KeyValue) (interface{}, error) {
	value := string(kvs[0].Value)

	// priority parse float
	fv, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return fv, err
	}

	// then bool
	bv, err := strconv.ParseBool(value)
	if err == nil {
		return bv, err
	}

	// must string
	return value, nil
}

// kvParseMap ...
func (c *Client) kvParseMap(kvs []*mvccpb.KeyValue) (map[string]interface{}, error) {
	ret := make(map[string]interface{})

	kvsLen := len(kvs)

	for i := 0; i < kvsLen; {
		kv := kvs[i]
		key := string(kv.Key)
		nb := c.nextBranch(key)
		np := c.nextPath(key)

		j := i + 1
		for ; j < kvsLen; j++ {
			jkey := string(kvs[j].Key)
			if !strings.HasPrefix(jkey, np) {
				break
			}
		}

		sc, err := c.shadowClone(nb, nb)
		if err != nil {
			return nil, err
		}

		iv, err := sc.kvParse(kvs[i:j])
		if err != nil {
			return nil, err
		}

		ret[nb] = iv
		i = j
	}

	return ret, nil
}

// kvParseSlice ...
func (c *Client) kvParseSlice(kvs []*mvccpb.KeyValue) ([]interface{}, error) {
	ret := make([]interface{}, 0)

	kvsLen := len(kvs)

	for i := 0; i < kvsLen; {
		kv := kvs[i]
		key := string(kv.Key)
		nb := c.nextBranch(key)
		np := c.nextPath(key)

		j := i + 1
		for ; j < kvsLen; j++ {
			jkey := string(kvs[j].Key)
			if !strings.HasPrefix(jkey, np) {
				break
			}
		}

		sc, err := c.shadowClone(strconv.Itoa(i), nb)
		if err != nil {
			return nil, err
		}
		iv, err := sc.kvParse(kvs[i:j])
		if err != nil {
			return nil, err
		}

		ret = append(ret, iv)

		i = j
	}
	return ret, nil
}

// kvParseInvlid ...
func (c *Client) kvParseInvlid(kvs []*mvccpb.KeyValue) (interface{}, error) {
	kvsLen := len(kvs)

	// next branch range map
	nbRangeMap := make(map[string]struct{ s, e int })
	kind := Slice
	for i := 0; i < kvsLen; {
		kv := kvs[i]
		key := string(kv.Key)
		nb := c.nextBranch(key)
		np := c.nextPath(key)

		if kind != Map {
			iv, err := strconv.ParseInt(nb, 10, 0)
			if err != nil {
				kind = Map
			} else {
				if int(iv) != i {
					kind = Map
				}
			}
		}
		j := i + 1
		for ; j < kvsLen; j++ {
			jkey := string(kvs[j].Key)
			if !strings.HasPrefix(jkey, np) {
				break
			}
		}

		nbRangeMap[nb] = struct{ s, e int }{i, j}

		i = j
	}

	var ret interface{}
	if kind == Map {
		ret = make(map[string]interface{})
	} else {
		ret = make([]interface{}, 0)
	}

	for nb, rag := range nbRangeMap {
		sc, err := c.shadowClone(nb, nb)
		if err != nil {
			return nil, err
		}
		iv, err := sc.kvParse(kvs[rag.s:rag.e])
		if err != nil {
			return nil, err
		}

		if kind == Map {
			ret.(map[string]interface{})[nb] = iv
		} else {
			ret = append(ret.([]interface{}), iv)
		}
	}

	return ret, nil
}

// nextBranch ...
func (c *Client) nextBranch(fullPath string) string {
	path := c.rdir
	suffix := strings.TrimPrefix(fullPath, path)
	suffix = strings.Trim(suffix, "/")
	branches := strings.Split(suffix, "/")

	return branches[0]
}

// nextPath ...
func (c *Client) nextPath(fullPath string) string {
	return dir.Join(c.rdir, c.nextBranch(fullPath))
}
