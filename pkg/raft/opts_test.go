package raft

import "testing"

func TestListenAddr(t *testing.T) {
	withoutError(t, NodeUrl("http://web.site:9090"))
	withError(t, NodeUrl("web.site:9090"))
	withError(t, NodeUrl("web.site"))
	withError(t, NodeUrl("ftp://web.site"))
	withError(t, NodeUrl("  "))
	withError(t, NodeUrl("::"))
	withError(t, NodeUrl("http://web site:9090"))
}

func TestLogDir(t *testing.T) {
	withoutError(t, LogDir("/folder/foo/dir"))
	withError(t, LogDir("  "))
}

func TestSnapDir(t *testing.T) {
	withoutError(t, SnapDir("/folder/foo/dir"))
	withError(t, SnapDir("  "))
}

func TestClusterUrl(t *testing.T) {
	withoutError(t, ClusterUrl("http://site1:9090,http://site2:9090,http://site3:9090"))
	withError(t, ClusterUrl("http://site1:9090,site2:9090,http://site3:9090"))
	withError(t, ClusterUrl("http://site1:9090,http://site2:9090,http://site3 :9090"))
	withError(t, ClusterUrl("  "))
}

func TestJoin(t *testing.T) {
	clusUrl := "http://site1:9090,http://site2:9090,http://site3:9090"
	nodeUrl := "http://site2:9090"
	if opts, err := NewOptions(NodeUrl(nodeUrl), ClusterUrl(clusUrl)); err != nil {
		t.Errorf("Expected no error but got: %v", err)
	} else {
		join := opts.Join()
		if join {
			t.Errorf("Expected join flag to be false")
		}
	}

	nodeUrl = "http://site4:9090"
	if opts, err := NewOptions(NodeUrl(nodeUrl), ClusterUrl(clusUrl)); err != nil {
		t.Errorf("Expected no error but got: %v", err)
	} else {
		join := opts.Join()
		if !join {
			t.Errorf("Expected join flag to be true")
		}
	}
}

func TestDiscoverAddr(t *testing.T) {
	if opts, err := NewOptions(ClusterUrl("http://127.0.0.1:9090,http://site2:9090,http://site3:9090"), NodeUrl("")); err != nil {
		t.Errorf("Expected no error but got: %v", err)
	} else {
		join := opts.Join()
		if join {
			t.Errorf("Expected join flag to be false")
		}
	}
}

func TestClusterId(t *testing.T) {
	if opts, err := NewOptions(ClusterUrl("http://127.0.0.1:9090,http://site2:9090,http://site3:9090"), NodeUrl("")); err != nil {
		t.Errorf("Expected no error but got: %v", err)
	} else {
		if opts.ClusterId() != 0 {
			t.Errorf("Expected clusterID to be 0 when clusterName is not provided. Got %d ", opts.ClusterId())
		}
	}

	if opts, err := NewOptions(ClusterName("some-name")); err != nil {
		t.Errorf("Expected no error but got: %v", err)
	} else {
		if opts.ClusterId() == 0 {
			t.Errorf("Expected clusterID to be non 0 when clusterName is provided. Got %d ", opts.ClusterId())
		}
	}
}

func withError(t *testing.T, opt Option) {
	if _, err := NewOptions(opt); err != nil {
		t.Logf("As expected, received error: %v", err)
	} else {
		t.Errorf("Expected error but got none")
	}
}

func withoutError(t *testing.T, opt Option) {
	if _, err := NewOptions(opt); err != nil {
		t.Errorf("Expected no error but got %v", err)
	}
}
