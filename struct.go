package setcd

import (
	"fmt"
	"reflect"

	"github.com/coreos/etcd/clientv3"
	"github.com/fatih/structs"
	"github.com/mitchellh/mapstructure"
)

func (c *Client) GetStructVar(out interface{}, oos ...OpOption) error {
	outv := reflect.ValueOf(out)
	if outv.Kind() != reflect.Ptr {
		return fmt.Errorf("required a ponter of struct")
	}

	if outv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("required a ponter of struct")
	}

	mapv, err := c.GetMap(oos...)
	if err != nil {
		return err
	}
	if err := mapstructure.Decode(mapv, out); err != nil {
		return err
	}

	return nil
}

// putStruct ...
func (c *Client) putStruct(in interface{}) (resp *clientv3.PutResponse, err error) {
	return c.putMap(structs.New(in).Map())
}
