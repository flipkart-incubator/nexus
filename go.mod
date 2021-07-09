module github.com/flipkart-incubator/nexus

go 1.13

require (
	github.com/coreos/etcd v3.3.17+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20180511133405-39ca1b05acc7 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.5.0
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/shamaton/msgpack v1.2.1 // indirect
	github.com/smira/go-statsd v1.3.1
	github.com/vmihailenco/msgpack/v5 v5.3.4 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.uber.org/zap v1.13.0 // indirect
	golang.org/x/sys v0.0.0-20200124204421-9fbb57f87de9 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	google.golang.org/grpc v1.25.1
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.2.7 // indirect
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
	golang.org/x/sys => github.com/golang/sys v0.0.0-20191120155948-bd437916bb0e
	golang.org/x/text => github.com/golang/text v0.3.0
	golang.org/x/time => github.com/golang/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools => github.com/golang/tools v0.0.0-20191029190741-b9c20aec41a5
	golang.org/x/xerrors => github.com/golang/xerrors v0.0.0-20190717185122-a985d3407aa7
	honnef.co/go/tools => github.com/dominikh/go-tools v0.0.1-2019.2.3
)
