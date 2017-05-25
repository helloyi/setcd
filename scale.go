package setcd

import (
	"fmt"
	"strconv"

	"github.com/coreos/etcd/clientv3"
)

func (s *STM) putString(sv string) error {
	kind := s.mdGetKind()
	if kind != Nil && kind != Scale {
		return fmt.Errorf("invalid scale type on '%s'", s.odir)
	}
	s.stm.Put(s.rdir, sv)
	return nil
}

func (s *STM) putBool(bv bool) error {
	return s.putString(strconv.FormatBool(bv))
}

func (s *STM) putInt(iv int64) error {
	return s.putString(strconv.FormatInt(iv, 10))
}

func (s *STM) putUint(uv uint64) error {
	return s.putString(strconv.FormatUint(uv, 10))
}

func (s *STM) putFloat(fv float64) error {
	// TODO:
	// precision of fv
	return s.putString(strconv.FormatFloat(fv, 'E', -1, 64))
}

//------------
// get function
//------------

// GetBool ...
func (c *Client) GetBool() (bool, error) {
	getResp, err := c.Client.Get(c.ctx, c.rdir)
	if err != nil {
		return false, err
	}
	if getResp.Count != 1 {
		return false, fmt.Errorf("invalid bool type on %s", c.odir)
	}

	value := string(getResp.Kvs[0].Value)
	bv, err := strconv.ParseBool(value)
	if err != nil {
		return false, err
	}
	return bv, nil
}

// GetBoolVar ...
func (c *Client) GetBoolVar(bp *bool) error {
	bool, err := c.GetBool()
	if err != nil {
		return err
	}
	*bp = bool
	return nil
}

// GetInt ...
func (c *Client) getInt(base int, bitSize int) (int64, error) {
	getResp, err := c.Client.Get(c.ctx, c.rdir)
	if err != nil {
		return 0, err
	}
	if getResp.Count != 1 {
		return 0, fmt.Errorf("invalid path")
	}
	value := string(getResp.Kvs[0].Value)
	iv, err := strconv.ParseInt(value, base, bitSize)
	if err != nil {
		return 0, err
	}
	return iv, nil
}

// GetInt ...
func (c *Client) GetInt() (int, error) {
	iv, err := c.getInt(10, 0)
	if err != nil {
		return 0, err
	}
	return int(iv), err
}

// (c *Client) GetIntVar ...
func (c *Client) GetIntVar(ip *int) (err error) {
	*ip, err = c.GetInt()
	return
}

// GetInt8 ...
func (c *Client) GetInt8() (int8, error) {
	iv, err := c.getInt(10, 8)
	if err != nil {
		return 0, err
	}
	return int8(iv), err
}

// (c *Client) GetInt8Var ...
func (c *Client) GetInt8Var(ip *int8) (err error) {
	*ip, err = c.GetInt8()
	return
}

// GetInt16 ...
func (c *Client) GetInt16() (int16, error) {
	iv, err := c.getInt(10, 16)
	if err != nil {
		return 0, err
	}
	return int16(iv), err
}

// (c *Client) GetInt16Var ...
func (c *Client) GetInt16Var(ip *int16) (err error) {
	*ip, err = c.GetInt16()
	return
}

// GetInt32 ...
func (c *Client) GetInt32() (int32, error) {
	iv, err := c.getInt(10, 32)
	if err != nil {
		return 0, err
	}
	return int32(iv), err
}

// (c *Client) GetInt32Var ...
func (c *Client) GetInt32Var(ip *int32) (err error) {
	*ip, err = c.GetInt32()
	return
}

// GetInt64 ...
func (c *Client) GetInt64() (int64, error) {
	iv, err := c.getInt(10, 64)
	if err != nil {
		return 0, err
	}
	return int64(iv), err
}

// (c *Client) GetInt64Var ...
func (c *Client) GetInt64Var(ip *int64) (err error) {
	*ip, err = c.GetInt64()
	return
}

// GetUint ...
func (c *Client) getUint(base int, bitSize int) (uint64, error) {
	getResp, err := c.Client.Get(c.ctx, c.rdir)
	if err != nil {
		return 0, err
	}
	if getResp.Count != 1 {
		return 0, fmt.Errorf("invalid path")
	}
	value := string(getResp.Kvs[0].Value)
	iv, err := strconv.ParseUint(value, base, bitSize)
	if err != nil {
		return 0, err
	}
	return iv, nil
}

// GetUint ...
func (c *Client) GetUint() (uint, error) {
	iv, err := c.getUint(10, 0)
	if err != nil {
		return 0, err
	}
	return uint(iv), err
}

// GetUintVar ...
func (c *Client) GetUintVar(ip *uint) (err error) {
	*ip, err = c.GetUint()
	return
}

// GetUint8 ...
func (c *Client) GetUint8() (uint8, error) {
	iv, err := c.getUint(10, 8)
	if err != nil {
		return 0, err
	}
	return uint8(iv), err
}

// (c *Client) GetUint8Var ...
func (c *Client) GetUint8Var(ip *uint8) (err error) {
	*ip, err = c.GetUint8()
	return
}

// GetUint16 ...
func (c *Client) GetUint16() (uint16, error) {
	iv, err := c.getUint(10, 16)
	if err != nil {
		return 0, err
	}
	return uint16(iv), err
}

// (c *Client) GetUint16Var ...
func (c *Client) GetUint16Var(ip *uint16) (err error) {
	*ip, err = c.GetUint16()
	return
}

// GetUint32 ...
func (c *Client) GetUint32() (uint32, error) {
	iv, err := c.getUint(10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(iv), err
}

// (c *Client) GetUint32Var ...
func (c *Client) GetUint32Var(ip *uint32) (err error) {
	*ip, err = c.GetUint32()
	return
}

// GetUint64 ...
func (c *Client) GetUint64() (uint64, error) {
	iv, err := c.getUint(10, 64)
	if err != nil {
		return 0, err
	}
	return uint64(iv), err
}

// (c *Client) GetUint64Var ...
func (c *Client) GetUint64Var(ip *uint64) (err error) {
	*ip, err = c.GetUint64()
	return
}

// getFloat ...
func (c *Client) getFloat(bitSize int) (float64, error) {
	resp, err := c.Client.Get(c.ctx, c.rdir)
	if err != nil {
		return 0, err
	}
	if resp.Count == 0 {
		return 0, fmt.Errorf("invalid path")
	}
	value := string(resp.Kvs[0].Value)
	fv, err := strconv.ParseFloat(value, bitSize)
	if err != nil {
		return 0, err
	}
	return fv, nil
}

// GetFloat32 ...
func (c *Client) GetFloat32() (float32, error) {
	fv, err := c.getFloat(32)
	return float32(fv), err
}

// GetFloat32Var ...
func (c *Client) GetFloat32Var(fp *float32) (err error) {
	*fp, err = c.GetFloat32()
	return
}

// GetFloat64 ...
func (c *Client) GetFloat64() (float64, error) {
	return c.getFloat(64)
}

// GetFloat64Var ...
func (c *Client) GetFloat64Var(fp *float64) (err error) {
	*fp, err = c.GetFloat64()
	return
}

// GetString ...
func (c *Client) GetString() (string, error) {
	resp, err := c.Client.Get(c.ctx, c.rdir)
	if err != nil {
		return "", err
	}
	if resp.Count == 0 {
		return "", fmt.Errorf("invalid dir")
	}
	value := string(resp.Kvs[0].Value)
	return value, nil
}

// GetStringVar ...
func (c *Client) GetStringVar(sp *string) (err error) {
	*sp, err = c.GetString()
	return
}

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -----------------------------put functions-------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

// putString put string value to 'c.odir'
func (c *Client) putString(sv string) (*clientv3.PutResponse, error) {
	kind, err := c.mdGetKind()
	if err != nil {
		return nil, err
	}
	if kind != Nil && kind != Scale {
		return nil, fmt.Errorf("invalid scale type on '%s'", c.odir)
	}
	return c.Client.Put(c.ctx, c.rdir, sv)
}

// putBool put bool value to 'c.odir'
func (c *Client) putBool(bv bool) (*clientv3.PutResponse, error) {
	return c.putString(strconv.FormatBool(bv))
}

// putInt put int64 value to 'c.odir'
func (c *Client) putInt(iv int64) (*clientv3.PutResponse, error) {
	return c.putString(strconv.FormatInt(iv, 10))
}

// putUint put uint64 value to 'c.odir'
func (c *Client) putUint(uv uint64) (*clientv3.PutResponse, error) {
	return c.putString(strconv.FormatUint(uv, 10))
}

// putFloat put float64 value to 'c.odir'
//
// TODO:
// precision of fv
func (c *Client) putFloat(fv float64) (*clientv3.PutResponse, error) {
	return c.putString(strconv.FormatFloat(fv, 'E', -1, 64))
}
