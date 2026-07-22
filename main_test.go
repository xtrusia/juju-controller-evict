package main

import (
	"bytes"
	"strings"
	"testing"
)

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

func TestPrintPlanShowsExactMongoChanges(t *testing.T) {
	p := plan{
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
