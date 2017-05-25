// TODO:
// if the put value is equal to the stored value of dir

package setcd

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/helloyi/setcd/dir"
)

// Client provides and manages an mapetcd client session.
type Client struct {
	*clientv3.Client // etcd clientv3

	ctx  context.Context // etcd context
	odir string          // dir of user interface
	rdir string          // real path
	mdir string          // metadata path
}

// New creates a new mapetcd client
func New(cfg clientv3.Config, ctx context.Context, directory string) (*Client, error) {
	if !dir.IsAbs(directory) {
		return nil, ErrNotAbsoluteDir
	}

	odir := dir.Clean(directory)
	if odir == "/" {
		return nil, fmt.Errorf("%s: '%s'", ErrNotAllowedDir, "/")
	}
	if strings.HasPrefix(odir, Config.MD.RootDir) {
		return nil, fmt.Errorf("%s: '%s'", ErrNotAllowedDir, Config.MD.RootDir)
	}

	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	c := &Client{
		Client: etcdClient,
		ctx:    ctx,
	}

	rdir, err := c.realDir("/", "/", odir)
	if err != nil {
		return nil, err
	}
	mdir := dir.Join(Config.MD.RootDir, rdir)

	c.odir = odir
	c.rdir = rdir
	c.mdir = mdir

	return c, nil
}

// Close shuts down the client's etcd connections.
func (c *Client) Close() error {
	return c.Client.Close()
}

// ShadowClone "The Shadow Clone Jutsu"
// TODO:
// assert the client is New
func (c *Client) ShadowClone(directory string) (*Client, error) {
	newOdir := directory
	if !dir.IsAbs(newOdir) {
		newOdir = dir.Join(c.odir, newOdir)
	}
	newOdir = dir.Clean(newOdir)
	if newOdir == "/" {
		return nil, fmt.Errorf("%s: '%s'", ErrNotAllowedDir, "/")
	}
	if strings.HasPrefix(newOdir, Config.MD.RootDir) {
		return nil, fmt.Errorf("%s: '%s'", ErrNotAllowedDir, Config.MD.RootDir)
	}

	sc := &Client{
		Client: c.Client,
		ctx:    c.ctx,
	}

	newRdir, err := sc.realDir(c.rdir, c.odir, newOdir)
	if err != nil {
		return nil, err
	}
	newMdir := dir.Join(Config.MD.RootDir, newRdir)

	sc.odir = newOdir
	sc.rdir = newRdir
	sc.mdir = newMdir

	return sc, nil
}

// Get ...
// TODO
// withKeysOnly:
//  - [x] map
//  - [] array
// withTagsOnly
func (c *Client) Get(oos ...OpOption) (interface{}, error) {
	opt := parseOption(oos)

	if opt.tagsOnly {
		return c.mdGetTags()
	}

	etcdOpts := make([]clientv3.OpOption, 0)
	if opt.tag != "" {
		rev, err := c.mdGetRev(opt.tag)
		if err != nil {
			return nil, err
		}
		etcdOpts = append(etcdOpts, clientv3.WithRev(rev))
	}

	if opt.keysOnly {
		return c.mdGetIdxes(etcdOpts...)
	}

	etcdOpts = append(etcdOpts, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	resp, err := c.Client.Get(c.ctx, c.rdir, etcdOpts...)
	if err != nil {
		return nil, err
	}
	ret, err := c.kvParse(resp.Kvs)

	if opt.eval {
		return c.eval(ret, opt.evalTags, opt.evalVarFmt, opt.evalVarCheck)
	}

	return ret, err
}

// Put ...
func (c *Client) Put(in interface{}, oos ...OpOption) error {
	opt := parseOption(oos)

	if opt.lock {
		resp, err := concurrency.NewSTM(
			c.Client,
			func(stm concurrency.STM) error {
				s := newSTM(stm, c)
				return s.put(in)
			},
			concurrency.WithAbortContext(c.ctx),
			concurrency.WithIsolation(concurrency.RepeatableReads))

		if err != nil {
			return err
		}

		if opt.tag != "" {
			rev := resp.Header.Revision
			return c.mdPutTag(opt.tag, rev)
		}
		return nil
	}

	resp, err := c.put(in)
	if err != nil {
		return err
	}

	if opt.tag != "" {
		rev := resp.Header.Revision
		return c.mdPutTag(opt.tag, rev)
	}

	return nil
}

// Delete ...
func (c *Client) Delete(oos ...OpOption) error {
	opt := parseOption(oos)

	var opts []clientv3.Op
	if dir.Depth(c.rdir) == 1 {
		opts = []clientv3.Op{
			clientv3.OpDelete(c.rdir, clientv3.WithPrefix()),
			clientv3.OpDelete(c.mdir, clientv3.WithPrefix()),
		}
	} else {

		pc, err := c.shadowClone("../", "../")
		if err != nil {
			return err
		}

		pkind, err := pc.mdGetKind()
		if err != nil {
			return err
		}

		switch pkind {
		case Slice, Map:
			length, err := pc.mdGetLen()
			if err != nil {
				return err
			}
			if length == 0 {
				return fmt.Errorf("%s: '%s'", ErrEmptyDir, dir.Join(c.odir, "../"))
			}

			length -= 1

			ldir := pc.mdLenDir()
			idir := pc.mdIdxDir(dir.SubD(c.rdir, 1))

			opts = []clientv3.Op{
				clientv3.OpDelete(c.rdir, clientv3.WithPrefix()),    // delete data
				clientv3.OpDelete(c.mdir, clientv3.WithPrefix()),    // delete  matedata
				clientv3.OpDelete(idir),                             // delete idx
				clientv3.OpPut(ldir, strconv.FormatInt(length, 10)), //
			}
		case Scale:
			return fmt.Errorf("%s 'scale': '%s'", ErrUnsupportedDeletion, dir.Join(c.odir, ".."))
		case Nil:
			return fmt.Errorf("%s 'nil': '%s'", ErrUnsupportedDeletion, dir.Join(c.odir, ".."))
		case Invalid:
			return fmt.Errorf("%s 'invalid': '%s'", ErrUnsupportedDeletion, dir.Join(c.odir, ".."))
		default:
			return fmt.Errorf("%s: '%s'", ErrUnknownType, dir.Join(c.odir, ".."))
		}
	}

	resp, err := c.Client.Txn(c.ctx).Then(
		opts...,
	).Commit()
	if err != nil {
		return err
	}

	if opt.tag != "" {
		rev := resp.Header.Revision
		return c.mdPutTag(opt.tag, rev)
	}

	return nil
}

// Do calls function fn on each element of the map/slice.
func (c *Client) Do(fn func(string, interface{}) bool, oos ...OpOption) error {
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

	if kind != Slice && kind != Map {
		return fmt.Errorf("%s '%s': '%s'", ErrUnsupportedDo, kind.String(), c.odir)
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

		ri := key
		if kind == Slice {
			ri = strconv.Itoa(idx)
		}

		if ok := fn(ri, val); !ok {
			return nil
		}
	}
	return nil
}

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -----------------------------internal functions--------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

func (c *Client) shadowClone(odir, rdir string) (*Client, error) {
	sc := &Client{
		Client: c.Client,
		ctx:    c.ctx,
	}

	if dir.IsAbs(odir) && dir.IsAbs(rdir) {
		sc.odir = odir
		sc.rdir = rdir
	} else if !dir.IsAbs(odir) && !dir.IsAbs(rdir) {
		sc.odir = dir.Join(c.odir, odir)
		sc.rdir = dir.Join(c.rdir, rdir)
	} else {
		return nil, fmt.Errorf("not match between odir and rdir")
	}

	sc.odir = dir.Clean(sc.odir)
	sc.rdir = dir.Clean(sc.rdir)

	if sc.odir == "/" {
		return nil, fmt.Errorf("the root directory is not allowed to be manipulated")
	}
	if dir.Depth(sc.odir) != dir.Depth(sc.rdir) {
		return nil, fmt.Errorf("not match between odir and rdir")
	}

	sc.mdir = dir.Join(Config.MD.RootDir, sc.rdir)
	sc.mdir = dir.Clean(sc.mdir)

	return sc, nil
}

func (c *Client) put(in interface{}) (*clientv3.PutResponse, error) {
	v := reflect.ValueOf(in)
	switch v.Kind() {
	case reflect.Ptr:
		return c.put(v.Elem().Interface())

	case reflect.Bool:
		return c.putBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return c.putInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return c.putUint(v.Uint())
	case reflect.Float32, reflect.Float64:
		return c.putFloat(v.Float())
	case reflect.String:
		return c.putString(v.String())
	case reflect.Slice, reflect.Array:
		return c.putSlice(v.Interface())
	case reflect.Map:
		return c.putMap(v.Interface())
	case reflect.Struct:
		return c.putStruct(v.Interface())

	default:
		return nil, fmt.Errorf("%s: '%s'", ErrUnsupportedType, v.Kind().String())
	}
}

// getKeys ...
func (c *Client) getKeys() {

}

// eval ...
// TODO: eval tag
// header
func (c *Client) eval(val interface{}, etags map[string]string,
	varFmt func(string) string, varCheck func(string) error) (interface{}, error) {

	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String:
		sv := v.String()

		if !strings.Contains(sv, Config.Delimiters[0]) ||
			!strings.Contains(sv, Config.Delimiters[1]) {
			return sv, nil
		}

		// single template var, return origin value
		if strings.HasPrefix(sv, Config.Delimiters[0]) && strings.HasSuffix(sv, Config.Delimiters[1]) {
			// format var
			d := varFmt(sv)
			// check var
			if err := varCheck(d); err != nil {
				return nil, err
			}
			d = strings.TrimPrefix(d, Config.Delimiters[0])
			d = strings.TrimSuffix(d, Config.Delimiters[1])

			sc, err := c.ShadowClone(d)
			if err != nil {
				return nil, err
			}
			rootDir := dir.ParentD(d, 1)
			return sc.Get(WithEval(), WithTag(etags[rootDir]), WithEvalVarFmt(varFmt))
		}

		// multiple template vars, return combination of strings
		fields := strings.Split(sv, Config.Delimiters[0])
		for idx, field := range fields {
			fields2 := strings.Split(field, Config.Delimiters[1])
			if len(fields2) == 2 {
				tplVar := fields2[0]
				d := strings.TrimSuffix(tplVar, Config.Delimiters[1])

				// fmt var
				d = varFmt(fmt.Sprintf("{{%s}}", d))
				// check var
				if err := varCheck(d); err != nil {
					return nil, err
				}

				d = strings.TrimPrefix(d, Config.Delimiters[0])
				d = strings.TrimSuffix(d, Config.Delimiters[1])

				sc, err := c.ShadowClone(d)
				if err != nil {
					return nil, err
				}

				rootDir := dir.ParentD(d, 1)
				tplVal, err := sc.Get(WithEval(), WithTag(etags[rootDir]), WithEvalVarFmt(varFmt))
				if err != nil {
					return nil, err
				}

				fields[idx] = fmt.Sprintf("%v%s", tplVal, fields2[1])
			}
		}
		return strings.Join(fields, ""), nil

	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			ev, err := c.eval(elem.Interface(), etags, varFmt, varCheck)
			if err != nil {
				return nil, err
			}
			val.([]interface{})[i] = ev
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			value := v.MapIndex(key)
			ev, err := c.eval(value.Interface(), etags, varFmt, varCheck)
			if err != nil {
				return nil, err
			}
			val.(map[string]interface{})[key.String()] = ev
		}
	default:
		return val, nil
	}
	return val, nil
}

func (c *Client) realCurDir(prdir, podir, codir string) (string, error) {
	iv, err := strconv.ParseInt(codir, 10, 64)
	if err != nil {
		return codir, nil
	}

	pc, err := c.shadowClone(podir, prdir)
	if err != nil {
		return "", err
	}

	kind, err := pc.mdGetKind()
	if kind != Slice {
		return codir, nil
	}

	length, err := pc.mdGetLen()
	if err != nil {
		return "", err
	}
	if iv >= int64(length) {
		return "", ErrIndexOutOfRange
	}

	idx, err := pc.mdGetIdxWithOrder(iv)
	if err != nil {
		return "", err
	}
	return idx, nil
}

func (c *Client) realDir(rdir, odir, newOdir string) (string, error) {
	if !dir.IsAbs(rdir) || !dir.IsAbs(odir) || !dir.IsAbs(newOdir) {
		return "", ErrNotAbsoluteDir
	}
	if dir.Depth(rdir) != dir.Depth(odir) {
		return "", fmt.Errorf("not matched of real directory and origin directory")
	}

	if rdir == "/" {
		p := dir.ParentD(newOdir, 1)
		rdir = p
		odir = p
	}

	if strings.HasPrefix(odir, newOdir) {
		depth := dir.Depth(newOdir)
		rdir := dir.ParentD(rdir, depth)
		return rdir, nil

	} else if strings.HasPrefix(newOdir, odir) {
		subdir := strings.TrimPrefix(newOdir, odir)

		prdir := rdir
		podir := odir

		branches := dir.Branches(subdir)
		for _, branch := range branches {
			cobrc := branch
			crbrc, err := c.realCurDir(prdir, podir, cobrc)
			if err != nil {
				return "", err
			}
			prdir = dir.Join(prdir, crbrc)
			podir = dir.Join(podir, cobrc)
		}
		return prdir, nil

	} else {
		cp := dir.CParent(odir, newOdir)
		if cp == "" {
			return "", fmt.Errorf("never be here Client.realDir")
		} else if cp == "/" {
			p := dir.ParentD(newOdir, 1)
			odir = p
			rdir = p
		} else {
			depth := dir.Depth(cp)
			odir = dir.ParentD(odir, depth)
			rdir = dir.ParentD(rdir, depth)
		}
		return c.realDir(rdir, odir, newOdir)
	}
}

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -----------------------------STM's Implementations------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

// STM ...
type STM struct {
	stm concurrency.STM

	odir string
	rdir string
	mdir string
}

func newSTM(stm concurrency.STM, client *Client) *STM {
	return &STM{
		stm:  stm,
		odir: client.odir,
		rdir: client.rdir,
		mdir: client.mdir,
	}
}

// ShadowClone
func (s *STM) shadowClone(odir, rdir string) (*STM, error) {
	ss := &STM{
		stm: s.stm,
	}
	if dir.IsAbs(odir) && dir.IsAbs(rdir) {
		ss.odir = odir
		ss.rdir = rdir
	} else if !dir.IsAbs(odir) && !dir.IsAbs(rdir) {
		ss.odir = dir.Join(s.odir, odir)
		ss.rdir = dir.Join(s.rdir, rdir)
	} else {
		return nil, fmt.Errorf("not match between odir and rdir")
	}

	ss.odir = dir.Clean(ss.odir)
	ss.rdir = dir.Clean(ss.rdir)

	ss.mdir = dir.Join(Config.MD.RootDir, ss.rdir)
	ss.mdir = dir.Clean(ss.mdir)

	return ss, nil
}

// put ...
func (s *STM) put(in interface{}) error {
	v := reflect.ValueOf(in)
	switch v.Kind() {
	case reflect.Ptr:
		return s.put(v.Elem().Interface())

	case reflect.Bool:
		return s.putBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return s.putInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return s.putUint(v.Uint())
	case reflect.Float32, reflect.Float64:
		return s.putFloat(v.Float())
	case reflect.String:
		return s.putString(v.String())
	case reflect.Slice, reflect.Array:
		return s.putSlice(v.Interface())
	case reflect.Map:
		return s.putMap(v.Interface())

	default:
		return fmt.Errorf("%s: '%s'", ErrUnsupportedType, v.Kind().String())
	}
}
