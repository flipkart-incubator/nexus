module github.com/flipkart-incubator/nexus

go 1.13

require (
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golang/protobuf v1.5.2
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/smira/go-statsd v1.3.1
	go.etcd.io/etcd/client/pkg/v3 v3.5.0-rc.1
	go.etcd.io/etcd/pkg/v3 v3.5.0-rc.1
	go.etcd.io/etcd/raft/v3 v3.5.0-rc.1
	go.etcd.io/etcd/server/v3 v3.5.0-rc.1
	go.uber.org/zap v1.17.0 // indirect
	google.golang.org/grpc v1.38.0
)

replace (
	cloud.google.com/go => github.com/googleapis/google-cloud-go v0.26.0
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20190510104115-cbcb75029529
	golang.org/x/exp => github.com/golang/exp v0.0.0-20190121172915-509febef88a4
	golang.org/x/lint => github.com/golang/lint v0.0.0-20190930215403-16217165b5de
	golang.org/x/mod => github.com/golang/mod v0.0.0-20190513183733-4bf6d317e70e
	golang.org/x/net => github.com/golang/net v0.0.0-20190620200207-3b0461eec859
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.0.0-20180821212333-d2e6202438be
	golang.org/x/sync => github.com/golang/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/sys => github.com/golang/sys v0.0.0-20210611083646-a4fc73990273
	golang.org/x/text => github.com/golang/text v0.3.0
	golang.org/x/time => github.com/golang/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools => github.com/golang/tools v0.0.0-20191029190741-b9c20aec41a5
	golang.org/x/xerrors => github.com/golang/xerrors v0.0.0-20190717185122-a985d3407aa7
	honnef.co/go/tools => github.com/dominikh/go-tools v0.0.1-2019.2.3
)
