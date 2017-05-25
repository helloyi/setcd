package setcd_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"os"
	"os/exec"
	"path/filepath"

	"testing"
)

var etcdDataDir = filepath.Join(os.Getenv("GOPATH"), "src/github.com/helloyi/setcd/test/data")
var etcdServer *exec.Cmd

func TestSetcd(t *testing.T) {
	RegisterFailHandler(func(message string, callerSkip ...int) {
		GinkgoWriter.Write([]byte(message))
	})
	RunSpecs(t, "Setcd")
}

var _ = BeforeSuite(func() {
	if _, err := os.Stat(etcdDataDir); os.IsNotExist(err) {
		err := os.MkdirAll(etcdDataDir, 0700)
		Expect(err).NotTo(HaveOccurred())
	}

	etcdBin := os.Getenv("ETCD_BIN")
	if etcdBin == "" {
		etcdBin = "etcd"
	}

	etcdServer = exec.Command(etcdBin, "--data-dir", etcdDataDir)
	go func() {
		err := etcdServer.Run()
		Expect(err).NotTo(HaveOccurred())
	}()
})

var _ = AfterSuite(func() {
	err := etcdServer.Process.Kill()
	Expect(err).NotTo(HaveOccurred())

	err = exec.Command("rm", "-fr", etcdDataDir).Run()
	Expect(err).NotTo(HaveOccurred())
})
