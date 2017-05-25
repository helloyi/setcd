package setcd_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/helloyi/setcd"
)

var _ = Describe("Setcd", func() {
	var (
		cli  *setcd.Client
		data map[string]interface{}
	)

	BeforeEach(func() {
		var err error
		cli, err = setcd.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		}, context.Background(), "/Setcd")

		data = map[string]interface{}{
			"k1": 1234,
			"k2": true,
			"k3": "string",
			"k4": []string{"a", "b", "c"},
		}
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := cli.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	Specify("Put", func() {
		err := cli.Put(data)
		Expect(err).NotTo(HaveOccurred())
	})
	By("put data done")

	Specify("Get", func() {
		res, err := cli.Get()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(data))
	})
	By("get data done")
})
