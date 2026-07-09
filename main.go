// juju-controller-evict removes every trace of a permanently dead Juju HA
// controller machine, without reviving it.
//
// A powered-off controller cannot be removed with "juju remove-machine --force":
// the evacuateMachine cleanup waits forever for the dead machine agent to tear
// down its controller unit, and "juju remove-unit" refuses to touch units of the
// controller application. Separately, a Dqlite node is only ever removed by the
// departing node's own graceful handover, so the dead node stays a cluster
// member for good.
//
// This tool unblocks both, on a surviving controller machine:
//
//   - Mongo: deletes the dead machine's unit documents (and their statuses,
//     unit state and constraints) and decrements the application unit count, so
//     the evacuateMachine cleanup sees no units left and lets Juju retire the
//     machine on its own.
//   - Dqlite: removes the dead node from the Raft cluster.
//
// It is a support tool for a machine that is never coming back. It refuses to
// touch a controller that still answers, writes a JSON backup of every document
// it deletes or modifies, and does nothing at all unless -yes is given.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	dqlite "github.com/canonical/go-dqlite/v3/client"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	"gopkg.in/yaml.v3"
)

const (
	defaultDqlitePort = 17666
	defaultStatePort  = 37017
	mongoServerName   = "juju-mongodb"
	stateDB           = "juju"

	// downState is the MongoDB replica set member state for a member the
	// primary cannot reach. Confirmed against replSetGetStatus, which reports
	// state 8 with stateStr "(not reachable/healthy)".
	downState = 8

	// deadSamples is how many times the dead member's state is re-checked
	// before anything is deleted, so a transient blip cannot look like death.
	deadSamples  = 3
	deadInterval = 2 * time.Second

	// Juju machine lifecycle values stored in the "life" field.
	lifeAlive = 0
	lifeDying = 1
	lifeDead  = 2

	// After deleting the units the evacuate cleanup advances the machine to
	// Dying on its own timer; wait up to this long for it before setting Dead.
	dyingWait     = 3 * time.Minute
	dyingPollTick = 5 * time.Second
)

// unitDocCollections hold one document per unit, keyed by the unit name.
// Derived from the live schema: units, statuses (workload, agent, charm),
// unitstates and constraints all carry the unit name inside their _id.
var unitDocCollections = []string{"statuses", "unitstates", "constraints"}

type agentConf struct {
	ControllerCert string `yaml:"controllercert"`
	ControllerKey  string `yaml:"controllerkey"`
	CACert         string `yaml:"cacert"`
	StatePassword  string `yaml:"statepassword"`
	DqlitePort     int    `yaml:"dqlite-port"`
	StatePort      int    `yaml:"stateport"`
}

func (c *agentConf) dqlitePort() int {
	if c.DqlitePort > 0 {
		return c.DqlitePort
	}
	return defaultDqlitePort
}

func (c *agentConf) statePort() int {
	if c.StatePort > 0 {
		return c.StatePort
	}
	return defaultStatePort
}

type plan struct {
	Machine       string                 `json:"machine"`
	ModelUUID     string                 `json:"model_uuid"`
	MemberAddress string                 `json:"mongo_member_address"`
	DqliteAddress string                 `json:"dqlite_address"`
	DqliteNodeID  uint64                 `json:"dqlite_node_id"`
	Units         []string               `json:"units"`
	Delete        []deletion             `json:"delete"`
	AppDecrements map[string]int         `json:"application_unitcount_decrement"`
	MachineDocID  string                 `json:"machine_doc_id"`
	MachineDoc    map[string]interface{} `json:"machine_doc_before"`
}

type deletion struct {
	Collection string                 `json:"collection"`
	ID         string                 `json:"_id"`
	Doc        map[string]interface{} `json:"doc"`
}

func main() {
	machine := flag.String("machine", "", "machine id of the dead controller, e.g. 1 (omit to only report cluster state)")
	controller := flag.String("controller", "", "run from a Juju client: name of the controller to act on (default: the current controller). Ignored on a controller machine")
	agentConfPath := flag.String("agent-conf", "", "agent.conf of a surviving controller (default: autodetect under /var/lib/juju/agents)")
	clusterPath := flag.String("cluster", "/var/lib/juju/dqlite/cluster.yaml", "Dqlite cluster.yaml of a surviving controller")
	mongoCA := flag.String("mongo-ca", "/var/snap/juju-db/common/ca.crt", "CA certificate mongod was started with")
	mongoCert := flag.String("mongo-cert", "/var/snap/juju-db/common/server.pem", "certificate and key presented to mongod")
	backupPath := flag.String("backup", "juju-controller-evict-backup.json", "where to write the pre-change document backup")
	apply := flag.Bool("yes", false, "apply the changes; without this the tool only reports what it would do")
	skipMongo := flag.Bool("skip-mongo", false, "leave the Juju state documents alone")
	skipDqlite := flag.Bool("skip-dqlite", false, "leave the Dqlite cluster alone")
	timeout := flag.Duration("timeout", 2*time.Minute, "overall timeout")
	flag.Parse()

	// When not run on a controller machine, act as a driver: copy this binary
	// to a surviving controller over "juju scp", run it there, fetch the backup
	// and clean up. An explicit -agent-conf forces worker mode.
	if !onController() && *agentConfPath == "" {
		if err := drive(driveArgs{
			controller: *controller,
			machine:    *machine,
			apply:      *apply,
			skipMongo:  *skipMongo,
			skipDqlite: *skipDqlite,
			timeout:    *timeout,
		}); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(runArgs{
		machine:    *machine,
		agentConf:  *agentConfPath,
		cluster:    *clusterPath,
		mongoCA:    *mongoCA,
		mongoCert:  *mongoCert,
		backup:     *backupPath,
		apply:      *apply,
		skipMongo:  *skipMongo,
		skipDqlite: *skipDqlite,
		timeout:    *timeout,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// ---------- driver (runs on a Juju client) ----------

type driveArgs struct {
	controller string
	machine    string
	apply      bool
	skipMongo  bool
	skipDqlite bool
	timeout    time.Duration
}

func onController() bool {
	m, _ := filepath.Glob("/var/lib/juju/agents/machine-*/agent.conf")
	return len(m) > 0
}

func drive(a driveArgs) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding own path: %w", err)
	}

	controller := a.controller
	if controller == "" {
		controller, err = currentController()
		if err != nil {
			return err
		}
	}
	model := controller + ":controller"

	runner, err := pickRunner(model, a.machine)
	if err != nil {
		return err
	}
	fmt.Printf("driving through surviving controller machine %s of %q\n", runner, controller)

	const remoteBin = "/tmp/juju-controller-evict"
	const remoteBackup = "/tmp/juju-controller-evict-backup.json"

	fmt.Println("copying tool to the controller...")
	if err := juju("scp", "-m", model, self, runner+":"+remoteBin); err != nil {
		return fmt.Errorf("copying tool: %w", err)
	}
	defer func() {
		if err := juju("ssh", "-m", model, runner, "sudo rm -f "+remoteBin+" "+remoteBackup); err != nil {
			fmt.Fprintf(os.Stderr, "warning: leaving %s on %s: %v\n", remoteBin, runner, err)
		}
	}()

	remote := "sudo chmod +x " + remoteBin + " && sudo " + remoteBin
	if a.machine != "" {
		remote += " -machine " + a.machine
	}
	if a.apply {
		remote += " -yes"
	}
	if a.skipMongo {
		remote += " -skip-mongo"
	}
	if a.skipDqlite {
		remote += " -skip-dqlite"
	}
	remote += " -backup " + remoteBackup + " -timeout " + a.timeout.String()

	fmt.Println("running on the controller:")
	fmt.Println("----")
	if err := jujuStream("ssh", "-m", model, runner, remote); err != nil {
		return fmt.Errorf("running tool on %s: %w", runner, err)
	}
	fmt.Println("----")

	if a.apply && a.machine != "" && !a.skipMongo {
		local := "juju-controller-evict-backup-" + a.machine + ".json"
		if err := juju("scp", "-m", model, runner+":"+remoteBackup, local); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not fetch backup: %v\n", err)
		} else {
			fmt.Printf("backup fetched to %s\n", local)
		}
	}
	return nil
}

func currentController() (string, error) {
	out, err := exec.Command("juju", "controllers", "--format", "json").Output()
	if err != nil {
		return "", fmt.Errorf("listing controllers (is the juju client set up?): %w", err)
	}
	var r struct {
		CurrentController string `json:"current-controller"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return "", fmt.Errorf("parsing controllers: %w", err)
	}
	if r.CurrentController == "" {
		return "", fmt.Errorf("no current controller; pass -controller")
	}
	return r.CurrentController, nil
}

// pickRunner returns the id of a started controller machine other than the
// target, sorted for determinism.
func pickRunner(model, target string) (string, error) {
	out, err := exec.Command("juju", "status", "-m", model, "--format", "json").Output()
	if err != nil {
		return "", fmt.Errorf("getting controller status: %w", err)
	}
	var s struct {
		Machines map[string]struct {
			JujuStatus struct {
				Current string `json:"current"`
			} `json:"juju-status"`
		} `json:"machines"`
	}
	if err := json.Unmarshal(out, &s); err != nil {
		return "", fmt.Errorf("parsing controller status: %w", err)
	}
	var ids []string
	for id := range s.Machines {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if id == target {
			continue
		}
		if s.Machines[id].JujuStatus.Current == "started" {
			return id, nil
		}
	}
	return "", fmt.Errorf("no started controller machine other than %q found", target)
}

func juju(args ...string) error {
	out, err := exec.Command("juju", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("juju %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func jujuStream(args ...string) error {
	cmd := exec.Command("juju", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type runArgs struct {
	machine    string
	agentConf  string
	cluster    string
	mongoCA    string
	mongoCert  string
	backup     string
	apply      bool
	skipMongo  bool
	skipDqlite bool
	timeout    time.Duration
}

func run(a runArgs) error {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()

	if a.agentConf == "" {
		found, err := findAgentConf()
		if err != nil {
			return err
		}
		a.agentConf = found
	}
	localTag := filepath.Base(filepath.Dir(a.agentConf))
	conf, err := loadAgentConf(a.agentConf)
	if err != nil {
		return err
	}
	localMachine := strings.TrimPrefix(localTag, "machine-")
	if a.machine != "" && a.machine == localMachine {
		return fmt.Errorf("refusing to evict machine %s: that is the machine this tool is running on", a.machine)
	}

	session, err := dialMongo(localTag, conf, a.mongoCA, a.mongoCert)
	if err != nil {
		return err
	}
	defer session.Close()

	members, err := replSetMembers(session)
	if err != nil {
		return err
	}
	fmt.Println("mongo replica set:")
	printMembers(members)

	dqliteCli, err := dialDqlite(ctx, conf, a.cluster)
	if err != nil {
		return err
	}
	defer dqliteCli.Close()

	leader, err := dqliteCli.Leader(ctx)
	if err != nil {
		return fmt.Errorf("getting the Dqlite leader: %w", err)
	}
	nodes, err := dqliteCli.Cluster(ctx)
	if err != nil {
		return fmt.Errorf("listing the Dqlite cluster: %w", err)
	}
	fmt.Println("\ndqlite cluster:")
	printNodes(nodes, leader)

	if a.machine == "" {
		return nil
	}

	target, ok := memberForMachine(members, a.machine)
	if !ok {
		return fmt.Errorf("no Mongo replica set member is tagged with juju-machine-id %q", a.machine)
	}
	if err := confirmDead(session, a.machine); err != nil {
		return err
	}
	if target.Votes > 0 {
		return fmt.Errorf("member %s still has a vote; wait for the peer grouper to demote it", target.Address)
	}

	host, _, err := net.SplitHostPort(target.Address)
	if err != nil {
		return fmt.Errorf("parsing member address %q: %w", target.Address, err)
	}
	dqliteAddr := net.JoinHostPort(host, fmt.Sprint(conf.dqlitePort()))

	var dqliteNode *dqlite.NodeInfo
	if !a.skipDqlite {
		node, err := nodeForAddress(nodes, dqliteAddr)
		if err != nil {
			return err
		}
		if leader != nil && node.Address == leader.Address {
			return fmt.Errorf("refusing to remove Dqlite node %d (%s): it is the current leader", node.ID, node.Address)
		}
		if err := checkNotListening(dqliteAddr); err != nil {
			return err
		}
		dqliteNode = &node
	}

	p := plan{
		Machine:       a.machine,
		MemberAddress: target.Address,
		DqliteAddress: dqliteAddr,
		AppDecrements: map[string]int{},
	}
	if dqliteNode != nil {
		p.DqliteNodeID = dqliteNode.ID
	}
	if !a.skipMongo {
		if err := planMongo(session, a.machine, &p); err != nil {
			return err
		}
	}

	fmt.Printf("\nplan for dead controller machine %s (%s):\n", a.machine, host)
	printPlan(&p)

	if !a.apply {
		fmt.Println("\ndry run: nothing was changed. Re-run with -yes to apply.")
		return nil
	}
	if len(p.Delete) > 0 {
		if err := writeBackup(a.backup, &p); err != nil {
			return err
		}
		fmt.Printf("\nbackup of %d documents written to %s\n", len(p.Delete), a.backup)
	}

	if !a.skipMongo {
		if err := applyMongo(session, &p); err != nil {
			return err
		}
		fmt.Println("mongo: unit documents removed; waiting for Juju to retire the machine...")
		if err := advanceMachineDead(session, &p); err != nil {
			return err
		}
		fmt.Println("mongo: machine set Dead; the live provisioner will remove it")
	}
	if dqliteNode != nil {
		fmt.Printf("dqlite: removing node %d (%s)...\n", dqliteNode.ID, dqliteNode.Address)
		if err := dqliteCli.Remove(ctx, dqliteNode.ID); err != nil {
			return fmt.Errorf("removing Dqlite node %d: %w", dqliteNode.ID, err)
		}
		nodes, err = dqliteCli.Cluster(ctx)
		if err != nil {
			return fmt.Errorf("listing the Dqlite cluster after removal: %w", err)
		}
		fmt.Println("\ndqlite cluster after removal:")
		printNodes(nodes, leader)
	}

	fmt.Printf("\ndone. Watch 'juju status' until machine %s disappears, then re-run 'juju enable-ha' to restore three voters.\n", a.machine)
	return nil
}

// ---------- agent.conf ----------

func findAgentConf() (string, error) {
	matches, err := filepath.Glob("/var/lib/juju/agents/machine-*/agent.conf")
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no agent.conf under /var/lib/juju/agents; run this on a controller machine or pass -agent-conf")
	}
	return matches[0], nil
}

func loadAgentConf(path string) (*agentConf, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf agentConf
	if err := yaml.Unmarshal(data, &conf); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if conf.ControllerCert == "" || conf.StatePassword == "" {
		return nil, fmt.Errorf("%s is not a controller agent.conf", path)
	}
	return &conf, nil
}

// ---------- mongo ----------

func dialMongo(localTag string, conf *agentConf, caPath, certPath string) (*mgo.Session, error) {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("reading mongo CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificate found in %s", caPath)
	}
	cert, err := tls.LoadX509KeyPair(certPath, certPath)
	if err != nil {
		return nil, fmt.Errorf("loading mongo client certificate: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		ServerName:   mongoServerName,
	}
	addr := net.JoinHostPort("127.0.0.1", fmt.Sprint(conf.statePort()))
	info := &mgo.DialInfo{
		// Seed with the local mongod but let mgo discover the rest of the
		// replica set, so writes are routed to the primary even when the tool
		// runs on a secondary controller.
		Addrs:    []string{addr},
		Direct:   false,
		Timeout:  20 * time.Second,
		Database: "admin",
		Username: localTag,
		Password: conf.StatePassword,
		DialServer: func(server *mgo.ServerAddr) (net.Conn, error) {
			c, err := net.DialTimeout("tcp", server.TCPAddr().String(), 15*time.Second)
			if err != nil {
				return nil, err
			}
			tc := tls.Client(c, tlsConfig)
			if err := tc.Handshake(); err != nil {
				c.Close()
				return nil, err
			}
			return tc, nil
		},
	}
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, fmt.Errorf("connecting to mongo at %s: %w", addr, err)
	}
	session.SetMode(mgo.Strong, true)
	return session, nil
}

type member struct {
	ID        int
	Address   string
	State     int
	Health    float64
	Votes     int
	MachineID string
}

func replSetMembers(session *mgo.Session) ([]member, error) {
	var status struct {
		Members []struct {
			ID     int     `bson:"_id"`
			Name   string  `bson:"name"`
			State  int     `bson:"state"`
			Health float64 `bson:"health"`
		} `bson:"members"`
	}
	if err := session.DB("admin").Run(bson.D{{Name: "replSetGetStatus", Value: 1}}, &status); err != nil {
		return nil, fmt.Errorf("running replSetGetStatus: %w", err)
	}
	var conf struct {
		Config struct {
			Members []struct {
				ID    int               `bson:"_id"`
				Host  string            `bson:"host"`
				Votes int               `bson:"votes"`
				Tags  map[string]string `bson:"tags"`
			} `bson:"members"`
		} `bson:"config"`
	}
	if err := session.DB("admin").Run(bson.D{{Name: "replSetGetConfig", Value: 1}}, &conf); err != nil {
		return nil, fmt.Errorf("running replSetGetConfig: %w", err)
	}
	byID := map[int]member{}
	for _, m := range status.Members {
		byID[m.ID] = member{ID: m.ID, Address: m.Name, State: m.State, Health: m.Health}
	}
	var out []member
	for _, c := range conf.Config.Members {
		m := byID[c.ID]
		m.ID = c.ID
		m.Address = c.Host
		m.Votes = c.Votes
		m.MachineID = c.Tags["juju-machine-id"]
		out = append(out, m)
	}
	return out, nil
}

func memberForMachine(members []member, machine string) (member, bool) {
	for _, m := range members {
		if m.MachineID == machine {
			return m, true
		}
	}
	return member{}, false
}

// confirmDead re-reads the replica set status a few times. The target must be
// unhealthy and DOWN in every sample, so a member that is merely slow or
// briefly partitioned is never treated as dead.
func confirmDead(session *mgo.Session, machine string) error {
	for i := 0; i < deadSamples; i++ {
		if i > 0 {
			time.Sleep(deadInterval)
		}
		members, err := replSetMembers(session)
		if err != nil {
			return err
		}
		m, ok := memberForMachine(members, machine)
		if !ok {
			return fmt.Errorf("machine %s vanished from the replica set config while checking", machine)
		}
		if m.Health != 0 || m.State != downState {
			return fmt.Errorf("member %s is not dead (state=%d health=%v); refusing to evict", m.Address, m.State, m.Health)
		}
	}
	return nil
}

func planMongo(session *mgo.Session, machine string, p *plan) error {
	db := session.DB(stateDB)

	var units []map[string]interface{}
	if err := db.C("units").Find(bson.M{"machineid": machine}).All(&units); err != nil {
		return fmt.Errorf("finding units on machine %s: %w", machine, err)
	}
	if len(units) == 0 {
		return fmt.Errorf("no unit documents reference machine %s; nothing to clean up in Mongo", machine)
	}

	for _, u := range units {
		id, _ := u["_id"].(string)
		name, _ := u["name"].(string)
		if err := assertNoPendingTxn(u, "units", id); err != nil {
			return err
		}
		if p.ModelUUID == "" {
			p.ModelUUID, _ = u["model-uuid"].(string)
		}
		p.Units = append(p.Units, name)
		p.Delete = append(p.Delete, deletion{Collection: "units", ID: id, Doc: u})
		p.AppDecrements[strings.SplitN(name, "/", 2)[0]]++

		re := unitDocRegexp(name)
		for _, coll := range unitDocCollections {
			var docs []map[string]interface{}
			if err := db.C(coll).Find(bson.M{"_id": bson.M{"$regex": re}}).All(&docs); err != nil {
				return fmt.Errorf("finding %s documents for %s: %w", coll, name, err)
			}
			for _, d := range docs {
				did, _ := d["_id"].(string)
				if err := assertNoPendingTxn(d, coll, did); err != nil {
					return err
				}
				p.Delete = append(p.Delete, deletion{Collection: coll, ID: did, Doc: d})
			}
		}
	}

	// Snapshot the machine doc: we pull the units from its principals and later
	// advance its life, and the backup must capture the original.
	p.MachineDocID = p.ModelUUID + ":" + machine
	var mdoc map[string]interface{}
	if err := db.C("machines").FindId(p.MachineDocID).One(&mdoc); err != nil {
		return fmt.Errorf("reading machine %s doc: %w", machine, err)
	}
	if err := assertNoPendingTxn(mdoc, "machines", p.MachineDocID); err != nil {
		return err
	}
	p.MachineDoc = mdoc
	return nil
}

// unitDocRegexp matches an _id that contains the unit name as a whole segment,
// so controller/1 never matches controller/10.
func unitDocRegexp(name string) string {
	return "(:|#)" + regexp.QuoteMeta(name) + "($|#)"
}

// assertNoPendingTxn refuses to touch a document that mgo/txn is mid-way
// through changing; racing an in-flight transaction would corrupt state.
func assertNoPendingTxn(doc map[string]interface{}, coll, id string) error {
	q, ok := doc["txn-queue"]
	if !ok || q == nil {
		return nil
	}
	if arr, ok := q.([]interface{}); ok && len(arr) == 0 {
		return nil
	}
	return fmt.Errorf("%s/%s has a pending transaction (txn-queue not empty); retry once Juju settles", coll, id)
}

func applyMongo(session *mgo.Session, p *plan) error {
	db := session.DB(stateDB)
	for _, d := range p.Delete {
		if err := db.C(d.Collection).RemoveId(d.ID); err != nil && err != mgo.ErrNotFound {
			return fmt.Errorf("deleting %s/%s: %w", d.Collection, d.ID, err)
		}
	}
	for app, n := range p.AppDecrements {
		id := p.ModelUUID + ":" + app
		err := db.C("applications").UpdateId(id, bson.M{
			"$inc": bson.M{"unitcount": -n, "txn-revno": 1},
		})
		if err != nil {
			return fmt.Errorf("decrementing unitcount of %s: %w", id, err)
		}
	}
	// Pull the removed units from the machine's principals. Without this the
	// evacuate cleanup keeps trying to load the now-deleted units and errors,
	// so the machine never advances out of Alive.
	for _, name := range p.Units {
		err := db.C("machines").UpdateId(p.MachineDocID, bson.M{
			"$pull": bson.M{"principals": name},
			"$inc":  bson.M{"txn-revno": 1},
		})
		if err != nil {
			return fmt.Errorf("removing %s from machine principals: %w", name, err)
		}
	}
	return nil
}

// advanceMachineDead lets Juju's own evacuate cleanup advance the machine from
// Alive to Dying (which removes the controller reference and the Mongo member),
// then sets it Dead so the live provisioner removes the machine and all of its
// related documents with proper transactions. It never touches the heavy
// removal itself.
func advanceMachineDead(session *mgo.Session, p *plan) error {
	db := session.DB(stateDB)
	deadline := time.Now().Add(dyingWait)
	for {
		var m struct {
			Life int `bson:"life"`
		}
		err := db.C("machines").FindId(p.MachineDocID).One(&m)
		if err == mgo.ErrNotFound {
			return nil // already removed
		}
		if err != nil {
			return fmt.Errorf("reading machine life: %w", err)
		}
		switch m.Life {
		case lifeDying:
			// Assert still Dying so we do not race the cleanup.
			err := db.C("machines").Update(
				bson.M{"_id": p.MachineDocID, "life": lifeDying},
				bson.M{"$set": bson.M{"life": lifeDead}, "$inc": bson.M{"txn-revno": 1}},
			)
			if err != nil && err != mgo.ErrNotFound {
				return fmt.Errorf("setting machine Dead: %w", err)
			}
			return nil
		case lifeDead:
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("machine %s did not reach Dying within %s; is 'juju remove-machine --force' pending?", p.Machine, dyingWait)
		}
		time.Sleep(dyingPollTick)
	}
}

func writeBackup(path string, p *plan) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ---------- dqlite ----------

func dialDqlite(ctx context.Context, conf *agentConf, clusterPath string) (*dqlite.Client, error) {
	cert, err := tls.X509KeyPair([]byte(conf.ControllerCert), []byte(conf.ControllerKey))
	if err != nil {
		return nil, fmt.Errorf("parsing controller certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(conf.ControllerCert)) {
		return nil, fmt.Errorf("failed to append controller cert to pool")
	}
	cfg, err := dqliteTLSConfig(cert, pool)
	if err != nil {
		return nil, err
	}
	store, err := dqlite.NewYamlNodeStore(clusterPath)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", clusterPath, err)
	}
	dial := dqlite.DialFuncWithTLS(dqlite.DefaultDialFunc, cfg)
	cli, err := dqlite.FindLeader(ctx, store, dqlite.WithDialFunc(dial))
	if err != nil {
		return nil, fmt.Errorf("connecting to the Dqlite leader: %w", err)
	}
	return cli, nil
}

// dqliteTLSConfig reproduces go-dqlite's app.SimpleDialTLSConfig, inlined so
// this tool links only the pure-Go client package and not the cgo-backed app
// and node packages.
func dqliteTLSConfig(cert tls.Certificate, pool *x509.CertPool) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            pool,
		Certificates:       []tls.Certificate{cert},
		ClientSessionCache: tls.NewLRUClientSessionCache(0),
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parsing controller certificate: %w", err)
	}
	if len(parsed.DNSNames) == 0 {
		return nil, fmt.Errorf("controller certificate has no DNS extension")
	}
	cfg.ServerName = parsed.DNSNames[0]
	return cfg, nil
}

func nodeForAddress(nodes []dqlite.NodeInfo, address string) (dqlite.NodeInfo, error) {
	for _, n := range nodes {
		if n.Address == address {
			return n, nil
		}
	}
	var known []string
	for _, n := range nodes {
		known = append(known, n.Address)
	}
	return dqlite.NodeInfo{}, fmt.Errorf("no Dqlite node has address %s; cluster has %s", address, strings.Join(known, ", "))
}

// checkNotListening refuses to proceed when the target still accepts
// connections on its Dqlite port.
func checkNotListening(address string) error {
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return nil
	}
	conn.Close()
	return fmt.Errorf("%s still answers on its Dqlite port, so it is not dead", address)
}

// ---------- output ----------

func printMembers(members []member) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  ID\tADDRESS\tMACHINE\tSTATE\tHEALTH\tVOTES\t")
	for _, m := range members {
		fmt.Fprintf(w, "  %d\t%s\t%s\t%d\t%v\t%d\t\n", m.ID, m.Address, m.MachineID, m.State, m.Health, m.Votes)
	}
	w.Flush()
}

func printNodes(nodes []dqlite.NodeInfo, leader *dqlite.NodeInfo) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  ID\tADDRESS\tROLE\t")
	for _, n := range nodes {
		marker := ""
		if leader != nil && n.Address == leader.Address {
			marker = "leader"
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\n", n.ID, n.Address, n.Role, marker)
	}
	w.Flush()
}

func printPlan(p *plan) {
	if len(p.Units) > 0 {
		fmt.Printf("  mongo: remove units %s and %d related documents\n", strings.Join(p.Units, ", "), len(p.Delete)-len(p.Units))
		for app, n := range p.AppDecrements {
			fmt.Printf("  mongo: decrement %s unitcount by %d\n", app, n)
		}
	}
	if p.DqliteNodeID != 0 {
		fmt.Printf("  dqlite: remove node %d (%s)\n", p.DqliteNodeID, p.DqliteAddress)
	}
}
