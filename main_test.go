package main

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	dqlite "github.com/canonical/go-dqlite/v3/client"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
)

func TestLoadAgentConfRequiresControllerModel(t *testing.T) {
	path := t.TempDir() + "/agent.conf"
	if err := os.WriteFile(path, []byte("controllercert: cert\nstatepassword: password\nmodel: model-uuid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	conf, err := loadAgentConf(path)
	if err != nil {
		t.Fatalf("loading controller agent config: %v", err)
	}
	if conf.ModelUUID != "uuid" {
		t.Fatalf("model UUID = %q, want uuid", conf.ModelUUID)
	}

	if err := os.WriteFile(path, []byte("controllercert: cert\nstatepassword: password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAgentConf(path); err == nil {
		t.Fatal("expected an agent config without a model to be rejected")
	}
}

func TestWriteBackupIsPrivate(t *testing.T) {
	path := t.TempDir() + "/backup.json"
	if err := os.WriteFile(path, []byte("old backup"), 0o644); err != nil {
		t.Fatalf("creating backup: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("setting backup permissions: %v", err)
	}
	if err := writeBackup(path, &plan{}); err != nil {
		t.Fatalf("writing backup: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stating backup: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("backup permissions = %o, want 600", got)
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

	if err := validateMachinePrincipals(doc, []string{"controller/1", "controller/2"}); err != nil {
		t.Fatalf("validating machine principals: %v", err)
	}
	err := validateMachinePrincipals(doc, []string{"controller/1", "controller/3"})
	if err == nil || !strings.Contains(err.Error(), "controller/3") {
		t.Fatalf("got error %v, want missing controller/3", err)
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
