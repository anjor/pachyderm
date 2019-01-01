package server

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/client/pps"
	versionlib "github.com/pachyderm/pachyderm/src/client/version"
	tu "github.com/pachyderm/pachyderm/src/server/pkg/testutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/workload"

	"github.com/golang/snappy"
	"golang.org/x/crypto/ssh"
)

const (
	KB = 1024
	MB = 1024 * KB
)

var pachClient *client.APIClient
var getPachClientOnce sync.Once

func getPachClient(t testing.TB) *client.APIClient {
	getPachClientOnce.Do(func() {
		var err error
		if addr := os.Getenv("PACHD_PORT_650_TCP_ADDR"); addr != "" {
			pachClient, err = client.NewInCluster()
		} else {
			pachClient, err = client.NewOnUserMachine(false, "user")
		}
		require.NoError(t, err)
	})
	return pachClient
}

func collectCommitInfos(t testing.TB, commitInfoIter client.CommitInfoIterator) []*pfs.CommitInfo {
	var commitInfos []*pfs.CommitInfo
	for {
		commitInfo, err := commitInfoIter.Next()
		if err == io.EOF {
			return commitInfos
		}
		require.NoError(t, err)
		commitInfos = append(commitInfos, commitInfo)
	}
}

// TODO(msteffen) equivalent to funciton in src/server/auth/server/admin_test.go.
// These should be unified.
func RepoInfoToName(repoInfo interface{}) interface{} {
	return repoInfo.(*pfs.RepoInfo).Repo.Name
}

func TestExtractRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := getPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestExtractRestore_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	nCommits := 2
	r := rand.New(rand.NewSource(45))
	fileContent := workload.RandString(r, 40*MB)
	for i := 0; i < nCommits; i++ {
		_, err := c.StartCommit(dataRepo, "master")
		require.NoError(t, err)
		_, err = c.PutFile(dataRepo, "master", fmt.Sprintf("file-%d", i), strings.NewReader(fileContent))
		require.NoError(t, err)
		require.NoError(t, c.FinishCommit(dataRepo, "master"))
	}

	numPipelines := 3
	input := dataRepo
	for i := 0; i < numPipelines; i++ {
		pipeline := tu.UniqueString(fmt.Sprintf("TestExtractRestore%d", i))
		require.NoError(t, c.CreatePipeline(
			pipeline,
			"",
			[]string{"bash"},
			[]string{
				fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
			},
			&pps.ParallelismSpec{
				Constant: 1,
			},
			client.NewPFSInput(input, "/*"),
			"",
			false,
		))
		input = pipeline
	}

	commitIter, err := c.FlushCommit([]*pfs.Commit{client.NewCommit(dataRepo, "master")}, nil)
	require.NoError(t, err)
	commitInfos := collectCommitInfos(t, commitIter)
	require.Equal(t, numPipelines, len(commitInfos))

	ops, err := c.ExtractAll(false)
	require.NoError(t, err)
	require.NoError(t, c.DeleteAll())
	require.NoError(t, c.Restore(ops))

	commitIter, err = c.FlushCommit([]*pfs.Commit{client.NewCommit(dataRepo, "master")}, nil)
	require.NoError(t, err)
	commitInfos = collectCommitInfos(t, commitIter)
	require.Equal(t, numPipelines, len(commitInfos))

	bis, err := c.ListBranch(dataRepo)
	require.NoError(t, err)
	require.Equal(t, 1, len(bis))
}

func TestExtractRestoreHeadlessBranches(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := getPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestExtractRestore_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	// create a headless branch
	require.NoError(t, c.CreateBranch(dataRepo, "headless", "", nil))

	ops, err := c.ExtractAll(false)
	require.NoError(t, err)
	require.NoError(t, c.DeleteAll())
	require.NoError(t, c.Restore(ops))

	bis, err := c.ListBranch(dataRepo)
	require.NoError(t, err)
	require.Equal(t, 1, len(bis))
	require.Equal(t, "headless", bis[0].Branch.Name)
}

func TestExtractVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	c := getPachClient(t)
	require.NoError(t, c.DeleteAll())

	dataRepo := tu.UniqueString("TestExtractRestore_data")
	require.NoError(t, c.CreateRepo(dataRepo))

	r := rand.New(rand.NewSource(45))
	_, err := c.PutFile(dataRepo, "master", "file", strings.NewReader(workload.RandString(r, 40*MB)))
	require.NoError(t, err)

	pipeline := tu.UniqueString("TestExtractRestore")
	require.NoError(t, c.CreatePipeline(
		pipeline,
		"",
		[]string{"bash"},
		[]string{
			fmt.Sprintf("cp /pfs/%s/* /pfs/out/", dataRepo),
		},
		&pps.ParallelismSpec{
			Constant: 1,
		},
		client.NewPFSInput(dataRepo, "/*"),
		"",
		false,
	))

	ops, err := c.ExtractAll(false)
	require.NoError(t, err)
	require.True(t, len(ops) > 0)

	// Check that every Op looks right; the version set matches pachd's version
	for _, op := range ops {
		opV := reflect.ValueOf(op).Elem()
		var versions, nonemptyVersions int
		for i := 0; i < opV.NumField(); i++ {
			fDesc := opV.Type().Field(i)
			if !strings.HasPrefix(fDesc.Name, "Op") {
				continue
			}
			versions++
			if strings.HasSuffix(fDesc.Name,
				fmt.Sprintf("%d_%d", versionlib.MajorVersion, versionlib.MinorVersion)) {
				require.False(t, opV.Field(i).IsNil())
				nonemptyVersions++
			} else {
				require.True(t, opV.Field(i).IsNil())
			}
		}
		require.Equal(t, 1, nonemptyVersions)
		require.True(t, versions > 1)
	}
}

func TestMigrateFrom1_7(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// Clear pachyderm cluster (so that next cluster starts up in a clean environment)
	c := getPachClient(t)
	require.NoError(t, c.DeleteAll())

	// Confirm that test is running against minikube
	kubectlContext, err := tu.Cmd("kubectl", "config", "current-context").Output()
	require.NoError(t, err)
	if strings.TrimSpace(string(kubectlContext)) != "minikube" {
		t.Skipf("TestMigrateFrom1_7 can only run against minikube (current "+
			"context is %q), as it needs to copy blocks directly to object store",
			string(kubectlContext))
	}

	// Load blocks from frozen 1.7 cluster into minikube
	// 1. SSH into minikube
	minikubeIP, err := tu.Cmd("minikube", "ip").Output()
	minikubeIP = bytes.TrimSpace(minikubeIP)
	require.NoError(t, err)
	key, err := ioutil.ReadFile(path.Join(
		os.Getenv("HOME"), ".minikube/machines/minikube/id_rsa"))
	require.NoError(t, err)
	signer, err := ssh.ParsePrivateKey(key)
	require.NoError(t, err)
	config := &ssh.ClientConfig{
		User:            "docker",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	t.Logf(fmt.Sprintf("%s:22", string(minikubeIP)))
	sshClient, err := ssh.Dial("tcp", string(minikubeIP)+":22", config)
	require.NoError(t, err)

	// 2. copy tar-ed objects into minikube
	objectsTarPath := path.Join(os.Getenv("GOPATH"),
		"src/github.com/pachyderm/pachyderm/etc/testing/migration/1_7/diagonal.objects.tar")
	objectsTar, err := os.Open(objectsTarPath)
	require.NoError(t, err, "could not open %q; did you run "+
		"'cd etc/testing/migration/1_7 && tar -xjf diagonal.objects.tar', as in "+
		"etc/testing/travis.sh?", objectsTarPath)
	sesh, err := sshClient.NewSession()
	require.NoError(t, err)
	sesh.Stdin, sesh.Stdout, sesh.Stderr = objectsTar, os.Stdout, os.Stderr
	require.NoError(t, sesh.Run(
		"sudo rm -rf /var/pachyderm/pachd/pach/block || true; "+
			"sudo mkdir -p /var/pachyderm/pachd/pach/block && "+
			"sudo tar -C /var/pachyderm/pachd/pach/block --strip-components=5 -xvf -"))

	// Restore dumped metadata (now that objects are present)
	md, err := os.Open(path.Join(os.Getenv("GOPATH"),
		"src/github.com/pachyderm/pachyderm/etc/testing/migration/1_7/diagonal.metadata"))
	require.NoError(t, err)
	require.NoError(t, c.RestoreReader(snappy.NewReader(md)))
	require.NoError(t, md.Close())

	// Wait for final imported commit to be processed
	commitIter, err := c.FlushCommit([]*pfs.Commit{client.NewCommit("left", "master")}, nil)
	require.NoError(t, err)
	commitInfos := collectCommitInfos(t, commitIter)
	// filter-left and filter-right both compute a join of left and
	// right--depending on when the final commit to 'left' was added, it may have
	// been processed multiple times (should be n * 3, as there are 3 pipelines)
	require.True(t, len(commitInfos) >= 3)

	// Inspect input
	commits, err := c.ListCommit("left", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 7, len(commits))
	commits, err = c.ListCommit("right", "master", "", 0)
	require.NoError(t, err)
	require.Equal(t, 7, len(commits))

	// Inspect output
	repos, err := c.ListRepo()
	require.NoError(t, err)
	require.ElementsEqualUnderFn(t,
		[]string{"left", "right", "filter-left", "filter-right", "join"},
		repos, RepoInfoToName)

	files, err := c.ListFile("join", "master", "/")
	require.NoError(t, err)
	fileNames := make(map[string]struct{})
	for _, fi := range files {
		fileNames[path.Base(fi.File.Path)] = struct{}{}
	}
	require.Equal(t, 100, len(fileNames)) // job processed all inputs

	// Confirm stats commits are present
	commits, err = c.ListCommit("join", "stats", "", 0)
	require.NoError(t, err)
	require.Equal(t, 14, len(commits))
}
