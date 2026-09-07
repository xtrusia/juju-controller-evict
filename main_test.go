package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	dqlite "github.com/canonical/go-dqlite/v3/client"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
)

func TestLoadAgentConfRequiresControllerModel(t *testing.T) {
	path := t.TempDir() + "/agent.conf"
	if err := os.WriteFile(path, []byte("cacert: ca\ncontrollercert: cert\nstatepassword: password\nmodel: model-uuid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf, err := loadAgentConf(path)
	if err != nil {
		t.Fatalf("loading controller agent config: %v", err)
	}
	if conf.ModelUUID != "uuid" {
		t.Fatalf("model UUID = %q, want uuid", conf.ModelUUID)
	}
	if conf.CACert != "ca" {
		t.Fatalf("CA certificate = %q, want ca", conf.CACert)
	}

	if err := os.WriteFile(path, []byte("controllercert: cert\nstatepassword: password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAgentConf(path); err == nil {
		t.Fatal("expected an agent config without a model to be rejected")
	}
}

func mongoTestCertificates(t *testing.T) (string, string, *x509.Certificate) {
	t.Helper()
	var ca *x509.Certificate
	var caKey ed25519.PrivateKey
	var caPEM, serverPEM string
	var leaf *x509.Certificate
	for i := int64(1); i <= 2; i++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(i), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			BasicConstraintsValid: true, IsCA: i == 1,
			KeyUsage: x509.KeyUsageDigitalSignature,
			DNSNames: []string{mongoServerName},
		}
		if template.IsCA {
			template.KeyUsage |= x509.KeyUsageCertSign
			ca, caKey = template, private
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caKey)
		if err != nil {
			t.Fatal(err)
		}
		encoded := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		if template.IsCA {
			caPEM = encoded
			continue
		}
		leaf, err = x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		key, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		serverPEM = encoded + string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	}
	return caPEM, serverPEM, leaf
}

func TestMongoTLSConfig(t *testing.T) {
	ca, server, leaf := mongoTestCertificates(t)
	otherCA, _, _ := mongoTestCertificates(t)
	tests := []struct {
		name, explicit, separate, agentCA, server, wantErr string
		caDirectory                                        bool
		untrusted                                          bool
	}{
		{name: "separate CA", separate: ca, server: server},
		{name: "agent CA", agentCA: ca, server: server},
		{name: "bundled CA", server: server + ca},
		{name: "CA before private key", server: strings.Replace(server, "-----BEGIN PRIVATE KEY-----", ca+"-----BEGIN PRIVATE KEY-----", 1)},
		{name: "explicit CA", explicit: "custom.pem", separate: ca, agentCA: "invalid", server: server},
		{name: "explicit bundle", explicit: "server.pem", server: server + ca},
		{name: "separate precedes agent", separate: ca, agentCA: "invalid", server: server},
		{name: "agent precedes bundle", agentCA: ca, server: server + otherCA},
		{name: "explicit missing does not fall back", explicit: "missing.pem", agentCA: ca, server: server + ca, wantErr: "reading mongo CA"},
		{name: "explicit invalid does not fall back", explicit: "custom.pem", separate: "invalid", agentCA: ca, server: server + ca, wantErr: "no CA certificate"},
		{name: "invalid separate does not fall back", separate: "invalid", agentCA: ca, server: server + ca, wantErr: "no CA certificate"},
		{name: "unreadable separate does not fall back", caDirectory: true, agentCA: ca, server: server + ca, wantErr: "reading mongo CA"},
		{name: "malformed certificate", separate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1, 2, 3}})), server: server + ca, wantErr: "parsing mongo CA"},
		{name: "invalid agent does not fall back", agentCA: "invalid", server: server + ca, wantErr: "no CA certificate"},
		{name: "leaf is not a CA", server: server, wantErr: "no CA certificate"},
		{name: "unrelated CA cannot verify server", separate: otherCA, server: server + ca, untrusted: true},
		{name: "missing client key", separate: ca, server: ca, wantErr: "loading mongo client certificate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			certPath := filepath.Join(dir, "server.pem")
			if err := os.WriteFile(certPath, []byte(test.server), 0o600); err != nil {
				t.Fatal(err)
			}
			caPath := ""
			if test.explicit != "" {
				caPath = filepath.Join(dir, test.explicit)
			}
			if test.caDirectory {
				if err := os.Mkdir(filepath.Join(dir, "ca.crt"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if test.separate != "" {
				path := filepath.Join(dir, "ca.crt")
				if caPath != "" {
					path = caPath
				}
				if err := os.WriteFile(path, []byte(test.separate), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := mongoTLSConfig(&agentConf{CACert: test.agentCA}, caPath, certPath)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("got error %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.InsecureSkipVerify || cfg.MinVersion != tls.VersionTLS12 || cfg.ServerName != mongoServerName {
				t.Fatal("Mongo TLS verification settings changed")
			}
			if len(cfg.Certificates) != 1 || !bytes.Equal(cfg.Certificates[0].Certificate[0], leaf.Raw) {
				t.Fatal("client certificate was not loaded")
			}
			_, err = leaf.Verify(x509.VerifyOptions{Roots: cfg.RootCAs, DNSName: cfg.ServerName})
			if (err != nil) != test.untrusted {
				t.Fatalf("server verification error = %v, want untrusted = %v", err, test.untrusted)
			}
			expectedRoots := x509.NewCertPool()
			trustedCA := ca
			if test.untrusted {
				trustedCA = otherCA
			}
			expectedRoots.AppendCertsFromPEM([]byte(trustedCA))
			if !cfg.RootCAs.Equal(expectedRoots) {
				t.Fatal("trust pool must contain only the selected CA, not the server certificate")
			}
		})
	}
}

func TestMongoPathFlags(t *testing.T) {
	for _, paths := range [][2]string{
		{},
		{"/path/ca.crt", "/path/server.pem"},
		{"/path with spaces/ca's.crt", "/path/$(printf expanded).pem"},
		{"", "/path/server.pem"},
	} {
		flags := mongoPathFlags(paths[0], paths[1])
		out, err := exec.Command("sh", "-c", "set --"+flags+"; for arg do printf '%s\\n' \"$arg\"; done").Output()
		if err != nil {
			t.Fatal(err)
		}
		var want string
		if paths[0] != "" {
			want += "-mongo-ca\n" + paths[0] + "\n"
		}
		if paths[1] != "" {
			want += "-mongo-cert\n" + paths[1] + "\n"
		}
		if string(out) != want {
			t.Fatalf("remote Mongo path arguments = %q, want %q", out, want)
		}
	}
}

func TestAssessForcedReplicaSetEviction(t *testing.T) {
	baseConfig := replicaSetConfig{
		Name:    "juju",
		Version: 7,
		Members: []replicaSetConfigMember{
			{ID: 1, Address: "10.0.0.1:37017"},
			{ID: 2, Address: "10.0.0.2:37017"},
			{ID: 3, Address: "10.0.0.3:37017"},
		},
	}
	baseSample := replicaSetStatus{Members: []replicaSetStatusMember{
		{ID: 1, State: primaryState, Health: 1},
		{ID: 2, State: secondaryState, Health: 1},
		{ID: 3, State: downState, Health: 0},
	}}
	repeat := func(sample replicaSetStatus) []replicaSetStatus {
		return []replicaSetStatus{sample, sample, sample}
	}

	tests := []struct {
		name    string
		config  replicaSetConfig
		samples []replicaSetStatus
		wantErr string
	}{
		{
			name:    "safe down target",
			config:  baseConfig,
			samples: repeat(baseSample),
		},
		{
			name:   "safe unknown target",
			config: baseConfig,
			samples: repeat(replicaSetStatus{Members: []replicaSetStatusMember{
				{ID: 1, State: primaryState, Health: 1},
				{ID: 2, State: secondaryState, Health: 1},
				{ID: 3, State: unknownState, Health: 0},
			}}),
		},
		{
			name:   "target recovers",
			config: baseConfig,
			samples: []replicaSetStatus{
				baseSample,
				{Members: []replicaSetStatusMember{
					{ID: 1, State: primaryState, Health: 1},
					{ID: 2, State: secondaryState, Health: 1},
					{ID: 3, State: secondaryState, Health: 1},
				}},
				baseSample,
			},
			wantErr: "did not remain DOWN or UNKNOWN",
		},
		{
			name:   "other voter stays unhealthy",
			config: baseConfig,
			samples: repeat(replicaSetStatus{Members: []replicaSetStatusMember{
				{ID: 1, State: primaryState, Health: 1},
				{ID: 2, State: downState, Health: 0},
				{ID: 3, State: downState, Health: 0},
			}}),
			wantErr: "voters [2] are unhealthy",
		},
		{
			name:   "no stable majority",
			config: baseConfig,
			samples: []replicaSetStatus{
				baseSample,
				{Members: []replicaSetStatusMember{
					{ID: 1, State: primaryState, Health: 1},
					{ID: 2, State: downState, Health: 0},
					{ID: 3, State: downState, Health: 0},
				}},
				baseSample,
			},
			wantErr: "majority needs 2",
		},
		{
			name: "no primary eligible member",
			config: replicaSetConfig{
				Name:    "juju",
				Version: 7,
				Members: []replicaSetConfigMember{
					{ID: 1, Address: "10.0.0.1:37017", Priority: float64Pointer(0)},
					{ID: 2, Address: "10.0.0.2:37017", Arbiter: boolPointer(true)},
					{ID: 3, Address: "10.0.0.3:37017"},
				},
			},
			samples: repeat(replicaSetStatus{Members: []replicaSetStatusMember{
				{ID: 1, State: secondaryState, Health: 1},
				{ID: 2, State: arbiterState, Health: 1},
				{ID: 3, State: downState, Health: 0},
			}}),
			wantErr: "no live primary-eligible member",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := assessForcedReplicaSetEviction(&test.config, 3, test.samples)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("assessment failed: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("got error %v, want one containing %q", err, test.wantErr)
			}
		})
	}
}

func TestReplicaSetSamplesHaveNoPrimary(t *testing.T) {
	withoutPrimary := []replicaSetStatus{
		{Members: []replicaSetStatusMember{{ID: 1, State: secondaryState, Health: 1}, {ID: 2, State: downState, Health: 0}}},
		{Members: []replicaSetStatusMember{{ID: 1, State: secondaryState, Health: 1}, {ID: 2, State: downState, Health: 0}}},
		{Members: []replicaSetStatusMember{{ID: 1, State: secondaryState, Health: 1}, {ID: 2, State: downState, Health: 0}}},
	}
	if !replicaSetSamplesHaveNoPrimary(withoutPrimary) {
		t.Fatal("samples without a primary were not detected")
	}

	withPrimary := append([]replicaSetStatus(nil), withoutPrimary...)
	withPrimary[1] = replicaSetStatus{Members: []replicaSetStatusMember{{ID: 1, State: primaryState, Health: 1}, {ID: 2, State: downState, Health: 0}}}
	if replicaSetSamplesHaveNoPrimary(withPrimary) {
		t.Fatal("samples containing a primary were reported as having none")
	}
}

func TestIsNoReachableServers(t *testing.T) {
	if !isNoReachableServers(fmt.Errorf("no reachable servers")) {
		t.Fatal("topology failure was not recognized")
	}
	for _, err := range []error{nil, fmt.Errorf("authentication failed"), fmt.Errorf("TLS handshake failed")} {
		if isNoReachableServers(err) {
			t.Fatalf("non-topology error %v was accepted", err)
		}
	}
}

func TestReconfigureReplicaSetWithoutMemberForced(t *testing.T) {
	runner := &recordingMongoRunner{}
	eviction := &replicaSetEviction{
		MemberID:      3,
		MemberAddress: "10.0.0.3:37017",
		Config: replicaSetConfig{
			Name:    "juju",
			Version: 7,
			Members: []replicaSetConfigMember{
				{ID: 1, Address: "10.0.0.1:37017"},
				{ID: 2, Address: "10.0.0.2:37017"},
				{ID: 3, Address: "10.0.0.3:37017"},
			},
		},
	}

	if err := reconfigureReplicaSetWithoutMember(runner, eviction, true); err != nil {
		t.Fatalf("forcing replica set member removal: %v", err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("got %d reconfig commands, want 1", len(runner.commands))
	}
	command := runner.commands[0]
	if len(command) != 2 || command[0].Name != "replSetReconfig" || command[1].Name != "force" || command[1].Value != true {
		t.Fatalf("unexpected command: %#v", command)
	}
	config, ok := command[0].Value.(replicaSetConfig)
	if !ok {
		t.Fatalf("unexpected config type %T", command[0].Value)
	}
	if config.Version != 8 || len(config.Members) != 2 {
		t.Fatalf("unexpected forced config: %#v", config)
	}
	for _, member := range config.Members {
		if member.ID == 3 {
			t.Fatalf("target member remains in forced config: %#v", config)
		}
	}
}

func TestReconfigureReplicaSetWithoutMemberNormal(t *testing.T) {
	runner := &recordingMongoRunner{}
	eviction := &replicaSetEviction{
		MemberID: 3,
		Config: replicaSetConfig{
			Version: 7,
			Members: []replicaSetConfigMember{
				{ID: 1, Address: "10.0.0.1:37017"},
				{ID: 2, Address: "10.0.0.2:37017"},
				{ID: 3, Address: "10.0.0.3:37017"},
			},
		},
	}

	if err := reconfigureReplicaSetWithoutMember(runner, eviction, false); err != nil {
		t.Fatalf("removing replica set member: %v", err)
	}
	if len(runner.commands) != 1 || len(runner.commands[0]) != 1 {
		t.Fatalf("normal reconfig unexpectedly used force: %#v", runner.commands)
	}
}

func TestIsQuorumCheckFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "not primary code", err: &mgo.QueryError{Code: 11602}},
		{name: "message", err: &mgo.QueryError{Message: "Quorum check failed"}, want: true},
		{name: "other query error", err: &mgo.QueryError{Code: 13}},
		{name: "plain error", err: fmt.Errorf("Quorum check failed")},
		{name: "typed nil", err: (*mgo.QueryError)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isQuorumCheckFailure(test.err); got != test.want {
				t.Fatalf("got %t, want %t", got, test.want)
			}
		})
	}
}

func TestReplicaSetConfigPreservesUnknownFields(t *testing.T) {
	original := bson.M{
		"_id":     "juju",
		"version": 7,
		"custom":  "preserved",
		"members": []interface{}{bson.M{
			"_id":    1,
			"host":   "10.0.0.1:37017",
			"hidden": true,
		}},
	}
	data, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("marshalling source config: %v", err)
	}
	var config replicaSetConfig
	if err := bson.Unmarshal(data, &config); err != nil {
		t.Fatalf("unmarshalling config: %v", err)
	}
	if config.Extra["custom"] != "preserved" || config.Members[0].Extra["hidden"] != true {
		t.Fatalf("unknown fields were not decoded: %#v", config)
	}

	data, err = bson.Marshal(config)
	if err != nil {
		t.Fatalf("marshalling config: %v", err)
	}
	var roundTrip map[string]interface{}
	if err := bson.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("unmarshalling round-trip config: %v", err)
	}
	if roundTrip["custom"] != "preserved" {
		t.Fatalf("unknown config field was not preserved: %#v", roundTrip)
	}
	members, ok := roundTrip["members"].([]interface{})
	if !ok || len(members) != 1 {
		t.Fatalf("unexpected round-trip members: %#v", roundTrip["members"])
	}
	member, ok := members[0].(map[string]interface{})
	if !ok || member["hidden"] != true {
		t.Fatalf("unknown member field was not preserved: %#v", members[0])
	}
}

func TestRemovedMemberAddress(t *testing.T) {
	doc := &machineNetworkDoc{
		Addresses: []machineAddress{{Value: "192.0.2.10", Scope: "local-cloud"}},
		MachineAddresses: []machineAddress{
			{Value: "127.0.0.1", Scope: "local-machine"},
			{Value: "10.0.0.3", Scope: "local-cloud"},
		},
	}
	nodes := []dqlite.NodeInfo{
		{ID: 1, Address: "10.0.0.1:17666"},
		{ID: 3, Address: "10.0.0.3:17666"},
	}

	address, alreadyRemoved, err := removedMemberAddress(doc, nodes, 37017)
	if err != nil {
		t.Fatalf("resolving removed member address: %v", err)
	}
	if alreadyRemoved {
		t.Fatal("Dqlite node was reported as already removed")
	}
	if address != "10.0.0.3:37017" {
		t.Fatalf("got address %q, want %q", address, "10.0.0.3:37017")
	}
}

func TestRemovedMemberAddressRejectsAmbiguousMatch(t *testing.T) {
	doc := &machineNetworkDoc{MachineAddresses: []machineAddress{
		{Value: "10.0.0.2", Scope: "local-cloud"},
		{Value: "10.0.0.3", Scope: "local-cloud"},
	}}
	nodes := []dqlite.NodeInfo{
		{ID: 2, Address: "10.0.0.2:17666"},
		{ID: 3, Address: "10.0.0.3:17666"},
	}

	_, _, err := removedMemberAddress(doc, nodes, 37017)
	if err == nil || !strings.Contains(err.Error(), "match 2 Dqlite nodes") {
		t.Fatalf("got error %v, want ambiguous match", err)
	}
}

func TestRemovedMemberAddressAllowsRemovedDqliteNode(t *testing.T) {
	doc := &machineNetworkDoc{MachineAddresses: []machineAddress{{
		Value: "10.0.0.3", Scope: "local-cloud",
	}}}
	nodes := []dqlite.NodeInfo{{ID: 1, Address: "10.0.0.1:17666"}}

	address, alreadyRemoved, err := removedMemberAddress(doc, nodes, 37017)
	if err != nil {
		t.Fatalf("resolving removed Dqlite node: %v", err)
	}
	if address != "" || !alreadyRemoved {
		t.Fatalf("got address %q, already removed %v", address, alreadyRemoved)
	}
}

type recordingMongoRunner struct {
	commands []bson.D
}

func (r *recordingMongoRunner) Run(command, result interface{}) error {
	data, ok := command.(bson.D)
	if !ok || len(data) == 0 {
		return fmt.Errorf("unexpected command %#v", command)
	}
	switch data[0].Name {
	case "replSetReconfig":
		r.commands = append(r.commands, data)
		return nil
	case "replSetGetStatus":
		status, ok := result.(*replicaSetStatus)
		if !ok {
			return fmt.Errorf("unexpected status result %T", result)
		}
		*status = replicaSetStatus{Members: []replicaSetStatusMember{{ID: 1, State: primaryState, Health: 1}}}
		return nil
	default:
		return fmt.Errorf("unexpected command %q", data[0].Name)
	}
}

func (*recordingMongoRunner) Refresh() {}

func boolPointer(value bool) *bool          { return &value }
func float64Pointer(value float64) *float64 { return &value }

func TestApplicationChangeFor(t *testing.T) {
	doc := map[string]interface{}{
		"_id":        "model-uuid:controller",
		"unitcount":  3,
		"txn-queue":  []interface{}{},
		"txn-revno":  int64(7),
		"model-uuid": "model-uuid",
	}

	change, err := applicationChangeFor("controller", "model-uuid:controller", 2, doc)
	if err != nil {
		t.Fatalf("applicationChangeFor returned an error: %v", err)
	}
	if change.UnitCountBefore != 3 || change.UnitCountAfter() != 1 {
		t.Fatalf("unexpected unitcount change: %#v", change)
	}
	if change.Doc["txn-revno"] != int64(7) {
		t.Fatalf("application document was not retained in the backup: %#v", change.Doc)
	}
}

func TestApplicationChangeForRejectsUnsafeDocuments(t *testing.T) {
	tests := []struct {
		name string
		doc  map[string]interface{}
		want string
	}{
		{
			name: "missing unitcount",
			doc:  map[string]interface{}{},
			want: "unitcount is missing",
		},
		{
			name: "insufficient unitcount",
			doc:  map[string]interface{}{"unitcount": 1},
			want: "cannot decrement by 2",
		},
		{
			name: "pending transaction",
			doc:  map[string]interface{}{"unitcount": 3, "txn-queue": []interface{}{map[string]interface{}{"id": "pending"}}},
			want: "pending transaction",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := applicationChangeFor("controller", "model-uuid:controller", 2, test.doc)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("got error %v, want one containing %q", err, test.want)
			}
		})
	}
}

func TestValidateMachinePrincipals(t *testing.T) {
	doc := map[string]interface{}{
		"principals": []interface{}{"controller/0", "controller/1", "controller/2"},
	}

	if err := validateMachinePrincipals(doc, []map[string]interface{}{{"name": "controller/1"}, {"name": "controller/2"}}); err != nil {
		t.Fatalf("validating machine principals: %v", err)
	}
	err := validateMachinePrincipals(doc, []map[string]interface{}{{"name": "controller/1"}, {"name": "controller/3"}})
	if err == nil || !strings.Contains(err.Error(), "controller/3") {
		t.Fatalf("got error %v, want missing controller/3", err)
	}
}

func TestValidateMachinePrincipalsWithSubordinates(t *testing.T) {
	tests := []struct {
		name    string
		units   []map[string]interface{}
		wantErr string
	}{
		{
			name: "subordinate shares the parent machine",
			units: []map[string]interface{}{
				{"name": "grafana-agent/10", "principal": "controller/2"},
				{"name": "controller/2", "principal": "", "subordinates": []interface{}{"grafana-agent/10"}},
			},
		},
		{
			name: "multiple subordinates",
			units: []map[string]interface{}{
				{"name": "controller/2", "subordinates": []string{"grafana-agent/10", "monitor/3"}},
				{"name": "grafana-agent/10", "principal": "controller/2"},
				{"name": "monitor/3", "principal": "controller/2"},
			},
		},
		{
			name:    "missing parent",
			units:   []map[string]interface{}{{"name": "grafana-agent/10", "principal": "controller/2"}},
			wantErr: "parent controller/2 is not among the units being removed",
		},
		{
			name: "parent absent from machine principals",
			units: []map[string]interface{}{
				{"name": "grafana-agent/10", "principal": "controller/9"},
				{"name": "controller/9", "subordinates": []string{"grafana-agent/10"}},
			},
			wantErr: "controller/9 is not present in principals",
		},
		{
			name: "parent does not reference subordinate",
			units: []map[string]interface{}{
				{"name": "controller/2", "subordinates": []string{"grafana-agent/11"}},
				{"name": "grafana-agent/10", "principal": "controller/2"},
			},
			wantErr: "grafana-agent/10 is not present in parent controller/2 subordinates",
		},
		{
			name: "parent is another subordinate",
			units: []map[string]interface{}{
				{"name": "grafana-agent/10", "principal": "controller/2"},
				{"name": "controller/2", "principal": "grafana-agent/10", "subordinates": []string{"grafana-agent/10"}},
			},
			wantErr: "parent controller/2 is itself a subordinate",
		},
		{
			name:    "invalid principal type",
			units:   []map[string]interface{}{{"name": "grafana-agent/10", "principal": 2}},
			wantErr: "principal is not a string",
		},
		{
			name: "invalid subordinate list",
			units: []map[string]interface{}{
				{"name": "controller/2", "subordinates": []interface{}{10}},
				{"name": "grafana-agent/10", "principal": "controller/2"},
			},
			wantErr: "subordinates contains a non-string value",
		},
		{
			name:    "principal unit still requires machine membership",
			units:   []map[string]interface{}{{"name": "grafana-agent/10", "principal": ""}},
			wantErr: "grafana-agent/10 is not present in principals",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMachinePrincipals(map[string]interface{}{"principals": []string{"controller/2"}}, test.units)
			if test.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("got error %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestSameMongoPlanDetectsStateChange(t *testing.T) {
	before := plan{
		Machine:      "1",
		ModelUUID:    "model-uuid",
		Units:        []string{"controller/1"},
		Delete:       []deletion{{Collection: "units", ID: "model-uuid:controller/1"}},
		Applications: []applicationChange{{Name: "controller", ID: "model-uuid:controller", UnitCountBefore: 3, Decrement: 1}},
		MachineDocID: "model-uuid:1",
		MachineDoc:   map[string]interface{}{"txn-revno": int64(4)},
	}
	after := before
	after.Applications = append([]applicationChange(nil), before.Applications...)
	after.Applications[0].UnitCountBefore = 2

	if sameMongoPlan(&before, &after) {
		t.Fatal("plans with different application unitcounts compare equal")
	}
	if !sameMongoPlan(&before, &before) {
		t.Fatal("identical plans compare different")
	}
}

func TestMongoCleanupOps(t *testing.T) {
	p := plan{
		Units: []string{"controller/1"},
		Delete: []deletion{{
			Collection: "units",
			ID:         "model-uuid:controller/1",
			Doc:        map[string]interface{}{"txn-revno": int64(7)},
		}},
		Applications: []applicationChange{{
			ID:              "model-uuid:controller",
			UnitCountBefore: 3,
			Decrement:       1,
			Doc:             map[string]interface{}{"txn-revno": int64(8)},
		}},
		MachineDocID: "model-uuid:1",
		MachineDoc:   map[string]interface{}{"txn-revno": int64(9)},
	}

	ops, err := mongoCleanupOps(&p)
	if err != nil {
		t.Fatalf("building cleanup transaction: %v", err)
	}
	if len(ops) != 3 {
		t.Fatalf("got %d operations, want 3", len(ops))
	}
	if !ops[0].Remove || !reflect.DeepEqual(ops[0].Assert, bson.D{{Name: "txn-revno", Value: int64(7)}}) {
		t.Fatalf("unexpected deletion operation: %#v", ops[0])
	}
	wantApplicationUpdate := bson.M{"$inc": bson.M{"unitcount": -1}}
	if !reflect.DeepEqual(ops[1].Update, wantApplicationUpdate) {
		t.Fatalf("application update = %#v, want %#v", ops[1].Update, wantApplicationUpdate)
	}
	wantMachineUpdate := bson.M{"$pullAll": bson.M{"principals": []string{"controller/1"}}}
	if !reflect.DeepEqual(ops[2].Update, wantMachineUpdate) {
		t.Fatalf("machine update = %#v, want %#v", ops[2].Update, wantMachineUpdate)
	}

	p.Delete[0].Doc = map[string]interface{}{}
	if _, err := mongoCleanupOps(&p); err == nil || !strings.Contains(err.Error(), "has no txn-revno") {
		t.Fatalf("got error %v, want missing txn-revno", err)
	}
}

func TestPrintPlanShowsExactMongoChanges(t *testing.T) {
	p := plan{
		ReplicaSetEviction: &replicaSetEviction{
			MemberID:      3,
			MemberAddress: "10.0.0.3:37017",
			Config:        replicaSetConfig{Version: 7},
		},
		Units: []string{"controller/1"},
		Delete: []deletion{
			{Collection: "statuses", ID: "model-uuid:controller/1"},
			{Collection: "units", ID: "model-uuid:controller/1"},
		},
		Applications: []applicationChange{{
			Name: "controller", ID: "model-uuid:controller", UnitCountBefore: 3, Decrement: 1,
		}},
		MachineDocID:  "model-uuid:1",
		DqliteNodeID:  7,
		DqliteAddress: "10.0.0.2:17666",
	}
	var out bytes.Buffer

	printPlan(&out, &p)
	got := out.String()
	for _, want := range []string{
		"mongo: remove replica set member 3 (10.0.0.3:37017), config version 7 -> 8; force only if the normal reconfig loses quorum",
		"mongo: delete statuses/model-uuid:controller/1",
		"mongo: delete units/model-uuid:controller/1",
		"mongo: update applications/model-uuid:controller unitcount 3 -> 2",
		"mongo: update machines/model-uuid:1 remove principals controller/1",
		"dqlite: remove node 7 (10.0.0.2:17666)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output %q does not contain %q", got, want)
		}
	}
}

func TestPrintPlanShowsForcedRemovalWithoutPrimary(t *testing.T) {
	p := plan{ReplicaSetEviction: &replicaSetEviction{
		MemberID:      2,
		MemberAddress: "10.0.0.2:37017",
		NoPrimary:     true,
		Config:        replicaSetConfig{Version: 8},
	}}
	var out bytes.Buffer

	printPlan(&out, &p)
	want := "mongo: force-remove replica set member 2 (10.0.0.2:37017), config version 8 -> 9; no primary is available"
	if !strings.Contains(out.String(), want) {
		t.Fatalf("output %q does not contain %q", out.String(), want)
	}
}
