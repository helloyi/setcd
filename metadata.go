package setcd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/helloyi/setcd/dir"
)

// mdGetLen ...
func (s *STM) mdGetLen() (int64, error) {
	return s.mdGetInt(Config.MD.LenSubDir)
}

// mdPutLen ...
func (s *STM) mdPutLen(len int64) error {
	return s.mdPutInt(Config.MD.LenSubDir, len)
}

// mdGetKind ...
func (s *STM) mdGetKind() Kind {
	skind := s.mdGetString(Config.MD.KindSubDir)

	kind := SKind(skind).ConvKind()
	if kind != Invalid { // only map or slice have metadata
		return kind
	}

	if s.stm.Rev(s.rdir) == 0 {
		return Nil
	}
	return Scale
}

// mdPutKind ...
func (s *STM) mdPutKind(k Kind) error {
	return s.mdPutString(Config.MD.KindSubDir, k.String())
}

func (s *STM) mdGetString(fieldDir string) string {
	key := dir.Join(s.mdir, fieldDir)
	val := s.stm.Get(key)
	return val
}

func (s *STM) mdGetInt(fieldDir string) (int64, error) {
	sv := s.mdGetString(fieldDir)
	if sv == "" {
		return 0, nil
	}
	fmt.Println("STM.mdGetInt", sv)
	return strconv.ParseInt(sv, 10, 64)
}

func (s *STM) mdPutString(fieldDir, sv string) error {
	key := dir.Join(s.mdir, fieldDir)
	s.stm.Put(key, sv)
	return nil
}

func (s *STM) mdPutInt(fieldDir string, iv int64) error {
	return s.mdPutString(fieldDir, strconv.FormatInt(iv, 10))
}

// mdGetLastID ...
func (s *STM) mdGetLastID() (int64, error) {
	return s.mdGetInt(Config.MD.LastIDSubDir)
}

// mdPutLastID ...
func (s *STM) mdPutLastID(id int64) error {
	return s.mdPutString(Config.MD.LastIDSubDir, strconv.FormatInt(id, 10))
}

// put index
func (s *STM) mdPutIdx(idx string) error {
	idxSubDir := dir.Join(Config.MD.IdxesSubDir, idx)
	return s.mdPutString(idxSubDir, idx)
}

//
// Client functions
//

func (c *Client) mdGetLen() (int64, error) {
	return c.mdGetInt(Config.MD.LenSubDir)
}

func (c *Client) mdGetKind(opts ...clientv3.OpOption) (Kind, error) {
	defaultOpts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithLimit(1),
	}
	opts = append(defaultOpts, opts...)

	resp, err := c.Client.Get(c.ctx, c.rdir, opts...)
	if err != nil {
		return Invalid, err
	}

	if resp.Count == 0 {
		return Nil, nil
	}

	key := string(resp.Kvs[0].Key)
	if c.rdir == key {
		return Scale, nil
	}

	sv, err := c.mdGetString(Config.MD.KindSubDir)
	if err != nil {
		return Invalid, err
	}
	return SKind(sv).ConvKind(), nil
}

// mdGetLastID ...
func (c *Client) mdGetLastID() (int64, error) {
	return c.mdGetInt(Config.MD.LastIDSubDir)
}

//
// Tag
//

func (c *Client) mdGetRev(tag string) (int64, error) {
	tagPath := c.mdGetTagPath(tag)

	resp, err := c.Client.Get(c.ctx, tagPath)
	if err != nil {
		return 0, err
	}

	if resp.Count == 0 {
		return 0, fmt.Errorf("the tag '%s' not exists", tag)
	}

	val := string(resp.Kvs[0].Value)
	rev, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		fmt.Println("Client.mdgetrev ", val)
		return 0, err
	}

	return rev, nil
}

func (c *Client) mdPutTag(tag string, rev int64) error {
	tagPath := c.mdGetTagPath(tag)
	kvc := clientv3.NewKV(c.Client)

	resp, err := kvc.Txn(c.ctx).
		If(clientv3.Compare(clientv3.Value(tagPath), "!=", "")).
		Else(clientv3.OpPut(tagPath, strconv.FormatInt(rev, 10))).
		Commit()

	if resp.Succeeded {
		return fmt.Errorf("the tag '%s' already exists", tag)
	}
	return err
}

// mdGetTagPath ...
func (c *Client) mdGetTagPath(tag string) string {
	// TODO:
	// bug: when the path is eq $root/$tagSubDir/$tag
	return dir.Join(c.mdGetTagRoot(), tag)
}

func (c *Client) mdGetTagRoot() string {
	tagRootBranches := strings.Split(c.mdir, "/")[:3]
	tagRoot := dir.Join(tagRootBranches...)
	tagRoot = dir.Join("/", tagRoot, Config.MD.TagsSubDir)
	return tagRoot
}

func (c *Client) mdGetTags() ([]string, error) {
	tagRoot := c.mdGetTagRoot()
	resp, err := c.Client.Get(c.ctx, tagRoot,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend))
	if err != nil {
		return nil, err
	}

	tags := make([]string, resp.Count)
	for i, kv := range resp.Kvs {
		key := string(kv.Key)
		tag := strings.TrimPrefix(key, tagRoot)
		tag = strings.Trim(tag, "/")
		tags[i] = tag
	}

	return tags, nil
}

func (c *Client) mdGetIdxes(opts ...clientv3.OpOption) ([]string, error) {
	idxDir := dir.Join(c.mdir, Config.MD.IdxesSubDir)

	etcdOpts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend),
	}
	etcdOpts = append(etcdOpts, opts...)
	resp, err := c.Client.Get(c.ctx, idxDir,
		etcdOpts...)
	if err != nil {
		return nil, err
	}

	idxes := make([]string, resp.Count)
	for i, kv := range resp.Kvs {
		key := string(kv.Key)
		idx := strings.TrimPrefix(key, idxDir)
		idx = strings.Trim(idx, "/")
		idxes[i] = idx
	}

	return idxes, nil
}

func (c *Client) mdGetIdxWithOrder(num int64) (string, error) {
	idxesDir := dir.Join(c.mdir, Config.MD.IdxesSubDir)
	startKey := dir.Join(idxesDir, "\x00")
	endKey := dir.Join(idxesDir, "\xFF")
	resp, err := c.Client.Get(c.ctx, startKey,
		clientv3.WithFromKey(),
		clientv3.WithRange(endKey),
		clientv3.WithLimit(num+1),
		clientv3.WithKeysOnly())
	if err != nil {
		return "", err
	}

	if num+1 > resp.Count {
		return "", fmt.Errorf("order '%d' out of range on '%s'", num, c.odir)
	}
	key := string(resp.Kvs[num].Key)
	idx := strings.TrimPrefix(key, idxesDir)
	idx = strings.Trim(idx, "/")

	return idx, nil
}

func (c *Client) mdGetString(fieldDir string) (string, error) {
	key := dir.Join(c.mdir, fieldDir)
	resp, err := c.Client.Get(c.ctx, key)
	if err != nil {
		return "", err
	}

	if resp.Count == 0 {
		return "", nil
	}

	val := string(resp.Kvs[0].Value)
	return val, nil
}

func (c *Client) mdDirExists(fieldDir string) (bool, error) {
	key := dir.Join(c.mdir, fieldDir)
	resp, err := c.Client.Get(c.ctx, key, clientv3.WithCountOnly(), clientv3.WithLimit(1))
	if err != nil {
		return false, err
	}
	if resp.Count != 1 {
		return false, nil
	}
	return true, nil
}

func (c *Client) mdPutString(fieldDir, val string) (*clientv3.PutResponse, error) {
	key := dir.Join(c.mdir, fieldDir)
	return c.Client.Put(c.ctx, key, val)
}

func (c *Client) mdGetInt(fieldDir string) (int64, error) {
	sv, err := c.mdGetString(fieldDir)
	if err != nil {
		return 0, err
	}
	if sv == "" {
		return 0, nil
	}

	return strconv.ParseInt(sv, 10, 64)
}

func (c *Client) mdPutInt(fieldDir string, val int64) (*clientv3.PutResponse, error) {
	return c.mdPutString(fieldDir, strconv.FormatInt(val, 10))
}

func (c *Client) mdIdxesDir() string {
	return dir.Join(c.mdir, Config.MD.IdxesSubDir)
}

func (c *Client) mdIdxDir(idx string) string {
	return dir.Join(c.mdIdxesDir(), idx)
}

func (c *Client) mdLenDir() string {
	return dir.Join(c.mdir, Config.MD.LenSubDir)
}

func (c *Client) mdPutIdx(idx string) (*clientv3.PutResponse, error) {
	idxSubDir := dir.Join(Config.MD.IdxesSubDir, idx)
	return c.mdPutString(idxSubDir, idx)
}

func (c *Client) mdIdxExists(idx string) (bool, error) {
	idxSubDir := dir.Join(Config.MD.IdxesSubDir, idx)
	return c.mdDirExists(idxSubDir)
}

func (c *Client) mdPutKind(k Kind) (*clientv3.PutResponse, error) {
	return c.mdPutString(Config.MD.KindSubDir, k.String())
}

func (c *Client) mdPutLastID(id int64) (*clientv3.PutResponse, error) {
	return c.mdPutString(Config.MD.LastIDSubDir, strconv.FormatInt(id, 10))
}

func (c *Client) mdPutLen(len int64) (*clientv3.PutResponse, error) {
	return c.mdPutInt(Config.MD.LenSubDir, len)
}
