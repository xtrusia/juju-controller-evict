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
//   - Mongo: removes the dead replica set member if its vote blocks Juju's
//     reconfig, falling back to a forced reconfig only after a quorum-check
//     failure. It then deletes the dead machine's unit documents and their
//     statuses, unit state and constraints, decrements the application unit
//     count, and pulls the units from the machine's principals. Juju's cleanup
//     advances the machine to Dying and removes the controller reference. The
//     tool sets the machine Dead so the live provisioner removes it and all of
//     its related documents with normal transactions. It does not rebuild the
//     machine removal by hand.
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
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	dqlite "github.com/canonical/go-dqlite/v3/client"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	"github.com/juju/mgo/v3/sstxn"
	"github.com/juju/mgo/v3/txn"
	"gopkg.in/yaml.v3"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const (
	defaultDqlitePort = 17666
	defaultStatePort  = 37017
	mongoServerName   = "juju-mongodb"
	stateDB           = "juju"
	controllersC      = "controllers"
	modelGlobalKey    = "e"

	// MongoDB replica set member states returned by replSetGetStatus.
	primaryState   = 1
	secondaryState = 2
	unknownState   = 6
	arbiterState   = 7
	downState      = 8

	// deadSamples is how many times the dead member's state is re-checked
	// before anything is deleted, so a transient blip cannot look like death.
	deadSamples  = 3
	deadInterval = 2 * time.Second

	// MongoDB elections commonly finish within 12 seconds. Poll past that
	// window before reporting a partial reconfiguration.
	primaryPollAttempts = 16
	primaryPollInterval = time.Second

	// Juju machine lifecycle values stored in the "life" field.
	lifeDying = 1
	lifeDead  = 2

	// After deleting the units the evacuate cleanup advances the machine to
	// Dying and removes its controller reference on its own timer. Wait for
	// both changes before setting the machine Dead.
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
	StatePassword  string `yaml:"statepassword"`
	ModelTag       string `yaml:"model"`
	ModelUUID      string `yaml:"-"`
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
	Machine            string                 `json:"machine"`
	ModelUUID          string                 `json:"model_uuid"`
	MemberAddress      string                 `json:"mongo_member_address"`
	ReplicaSetEviction *replicaSetEviction    `json:"replica_set_eviction,omitempty"`
	DqliteAddress      string                 `json:"dqlite_address"`
	DqliteNodeID       uint64                 `json:"dqlite_node_id"`
	Units              []string               `json:"units"`
	Delete             []deletion             `json:"delete"`
	Applications       []applicationChange    `json:"applications"`
	MachineDocID       string                 `json:"machine_doc_id"`
	MachineDoc         map[string]interface{} `json:"machine_doc_before"`
}

type replicaSetEviction struct {
	MemberID      int              `json:"member_id"`
	MemberAddress string           `json:"member_address"`
	NoPrimary     bool             `json:"no_primary"`
	Config        replicaSetConfig `json:"config_before"`
}

type replicaSetConfig struct {
	Name            string                   `bson:"_id" json:"name"`
	Version         int                      `bson:"version" json:"version"`
	Term            int                      `bson:"term,omitempty" json:"term,omitempty"`
	ProtocolVersion int                      `bson:"protocolVersion,omitempty" json:"protocol_version,omitempty"`
	Members         []replicaSetConfigMember `bson:"members" json:"members"`
	Extra           map[string]interface{}   `bson:",inline" json:"extra,omitempty"`
}

type replicaSetConfigMember struct {
	ID       int                    `bson:"_id" json:"id"`
	Address  string                 `bson:"host" json:"address"`
	Arbiter  *bool                  `bson:"arbiterOnly,omitempty" json:"arbiter_only,omitempty"`
	Priority *float64               `bson:"priority,omitempty" json:"priority,omitempty"`
	Tags     map[string]string      `bson:"tags,omitempty" json:"tags,omitempty"`
	Votes    *int                   `bson:"votes,omitempty" json:"votes,omitempty"`
	Extra    map[string]interface{} `bson:",inline" json:"extra,omitempty"`
}

type replicaSetStatus struct {
	Members []replicaSetStatusMember `bson:"members"`
}

type replicaSetStatusMember struct {
	ID      int     `bson:"_id"`
	Address string  `bson:"name"`
	State   int     `bson:"state"`
	Health  float64 `bson:"health"`
}

type machineNetworkDoc struct {
	Addresses        []machineAddress `bson:"addresses"`
	MachineAddresses []machineAddress `bson:"machineaddresses"`
}

type machineAddress struct {
	Value string `bson:"value"`
	Scope string `bson:"networkscope"`
}

type applicationChange struct {
	Name            string                 `json:"name"`
	ID              string                 `json:"_id"`
	UnitCountBefore int                    `json:"unitcount_before"`
	Decrement       int                    `json:"unitcount_decrement"`
	Doc             map[string]interface{} `json:"doc_before"`
}

func (c applicationChange) UnitCountAfter() int {
	return c.UnitCountBefore - c.Decrement
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
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

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
	keepRemoteBackup := false

	fmt.Println("copying tool to the controller...")
	if err := juju("scp", "-m", model, self, runner+":"+remoteBin); err != nil {
		return fmt.Errorf("copying tool: %w", err)
	}
	defer func() {
		cleanup := "sudo rm -f " + remoteBin
		if !keepRemoteBackup {
			cleanup += " " + remoteBackup
		}
		if err := juju("ssh", "-m", model, runner, cleanup); err != nil {
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
	runErr := jujuStream("ssh", "-m", model, runner, remote)
	fmt.Println("----")

	if a.apply && a.machine != "" && !a.skipMongo {
		local := "juju-controller-evict-backup-" + a.machine + ".json"
		if err := juju("scp", "-m", model, runner+":"+remoteBackup, local); err != nil {
			keepRemoteBackup = true
			fmt.Fprintf(os.Stderr, "warning: could not fetch backup: %v; backup retained at %s:%s\n", err, runner, remoteBackup)
		} else {
			fmt.Printf("backup fetched to %s\n", local)
		}
	}
	if runErr != nil {
		return fmt.Errorf("running tool on %s: %w", runner, runErr)
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

	session, directMongo, err := dialMongo(localTag, conf, a.mongoCA, a.mongoCert)
	if err != nil {
		return err
	}
	defer session.Close()
	if directMongo {
		fmt.Println("mongo: no replica set primary reachable; connected directly to the local member")
	}

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
	dqliteAlreadyRemoved := false
	if !ok {
		target, dqliteAlreadyRemoved, err = removedMemberForMachine(session, conf.ModelUUID, a.machine, nodes, conf.statePort())
		if err != nil {
			return fmt.Errorf("no Mongo replica set member is tagged with juju-machine-id %q and the removal cannot be resumed: %w", a.machine, err)
		}
	}
	var replicaSetEviction *replicaSetEviction
	if ok && target.Votes > 0 {
		if a.skipMongo {
			return fmt.Errorf("member %s still has a vote; cannot force-remove it with -skip-mongo", target.Address)
		}
		replicaSetEviction, err = planForcedReplicaSetEviction(session, target)
		if err != nil {
			return err
		}
	} else if ok {
		if err := confirmDead(session, a.machine); err != nil {
			return err
		}
	}
	if directMongo && !a.skipMongo && (replicaSetEviction == nil || !replicaSetEviction.NoPrimary) {
		return fmt.Errorf("direct Mongo connection is only permitted for a forced voter eviction when every status sample has no primary")
	}
	if target.Address == "" && !dqliteAlreadyRemoved {
		return fmt.Errorf("cannot determine the address of controller machine %s", a.machine)
	}

	host := "Dqlite node already removed"
	dqliteAddr := ""
	if target.Address != "" {
		host, _, err = net.SplitHostPort(target.Address)
		if err != nil {
			return fmt.Errorf("parsing member address %q: %w", target.Address, err)
		}
		dqliteAddr = net.JoinHostPort(host, fmt.Sprint(conf.dqlitePort()))
	}

	var dqliteNode *dqlite.NodeInfo
	if !a.skipDqlite && !dqliteAlreadyRemoved {
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
		Machine:            a.machine,
		ModelUUID:          conf.ModelUUID,
		MemberAddress:      target.Address,
		ReplicaSetEviction: replicaSetEviction,
		DqliteAddress:      dqliteAddr,
	}
	if dqliteNode != nil {
		p.DqliteNodeID = dqliteNode.ID
	}
	if !a.skipMongo {
		if err := planMongo(session, conf.ModelUUID, a.machine, &p); err != nil {
			return err
		}
	}

	fmt.Printf("\nplan for dead controller machine %s (%s):\n", a.machine, host)
	printPlan(os.Stdout, &p)

	if !a.apply {
		fmt.Println("\ndry run: nothing was changed. Re-run with -yes to apply.")
		return nil
	}
	if !a.skipMongo {
		if err := writeBackup(a.backup, &p); err != nil {
			return err
		}
		backupDocuments := len(p.Delete) + len(p.Applications) + 1
		if p.ReplicaSetEviction != nil {
			backupDocuments++
		}
		fmt.Printf("\nbackup of %d documents written to %s\n", backupDocuments, a.backup)
	}

	if !a.skipMongo {
		if p.ReplicaSetEviction != nil {
			if err := revalidateReplicaSetEviction(session, p.ReplicaSetEviction); err != nil {
				return err
			}
		}
		if err := revalidateMongoPlan(session, &p); err != nil {
			return err
		}
		if p.ReplicaSetEviction != nil {
			forced := false
			var err error
			if directMongo {
				forced = true
				err = reconfigureReplicaSetWithoutMember(session, p.ReplicaSetEviction, true)
			} else {
				err = reconfigureReplicaSetWithoutMember(session, p.ReplicaSetEviction, false)
				if isQuorumCheckFailure(err) {
					if err := revalidateReplicaSetEviction(session, p.ReplicaSetEviction); err != nil {
						return err
					}
					if err := revalidateMongoPlan(session, &p); err != nil {
						return err
					}
					err = reconfigureReplicaSetWithoutMember(session, p.ReplicaSetEviction, true)
					forced = true
				}
			}
			if err != nil {
				return err
			}
			if forced {
				reason := "after the normal reconfig lost quorum"
				if directMongo {
					reason = "because no primary was available"
				}
				fmt.Printf("mongo: replica set member %d (%s) force-removed %s\n", p.ReplicaSetEviction.MemberID, p.ReplicaSetEviction.MemberAddress, reason)
			} else {
				fmt.Printf("mongo: replica set member %d (%s) removed with a normal reconfig\n", p.ReplicaSetEviction.MemberID, p.ReplicaSetEviction.MemberAddress)
			}
		}
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

	fmt.Printf("\ndone. Watch 'juju status' until machine %s disappears, then run 'juju enable-ha -c <controller>' to restore three voters.\n", a.machine)
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
	if conf.ControllerCert == "" || conf.StatePassword == "" || conf.ModelTag == "" {
		return nil, fmt.Errorf("%s is not a controller agent.conf", path)
	}
	const modelTagPrefix = "model-"
	if !strings.HasPrefix(conf.ModelTag, modelTagPrefix) || len(conf.ModelTag) == len(modelTagPrefix) {
		return nil, fmt.Errorf("%s has an invalid model tag", path)
	}
	conf.ModelUUID = strings.TrimPrefix(conf.ModelTag, modelTagPrefix)
	return &conf, nil
}

// ---------- mongo ----------

func dialMongo(localTag string, conf *agentConf, caPath, certPath string) (*mgo.Session, bool, error) {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, false, fmt.Errorf("reading mongo CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, false, fmt.Errorf("no certificate found in %s", caPath)
	}
	cert, err := tls.LoadX509KeyPair(certPath, certPath)
	if err != nil {
		return nil, false, fmt.Errorf("loading mongo client certificate: %w", err)
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
		if !isNoReachableServers(err) {
			return nil, false, fmt.Errorf("connecting to mongo at %s: %w", addr, err)
		}
		replicaSetErr := err
		directInfo := *info
		directInfo.Direct = true
		session, err = mgo.DialWithInfo(&directInfo)
		if err != nil {
			return nil, false, fmt.Errorf("connecting to mongo at %s through the replica set failed (%v), and direct connection failed: %w", addr, replicaSetErr, err)
		}
		// A direct fallback must read status and run the forced reconfig on a
		// non-primary member. Monotonic keeps that direct socket usable until
		// the member becomes primary.
		session.SetMode(mgo.Monotonic, true)
		return session, true, nil
	}
	session.SetMode(mgo.Strong, true)
	return session, false, nil
}

func isNoReachableServers(err error) bool {
	return err != nil && err.Error() == "no reachable servers"
}

type member struct {
	ID        int
	Address   string
	State     int
	Health    float64
	Votes     int
	MachineID string
}

type mongoRunner interface {
	Run(command, result interface{}) error
	Refresh()
}

func currentReplicaSetStatus(session mongoRunner) (*replicaSetStatus, error) {
	var status replicaSetStatus
	if err := session.Run(bson.D{{Name: "replSetGetStatus", Value: 1}}, &status); err != nil {
		return nil, fmt.Errorf("running replSetGetStatus: %w", err)
	}
	return &status, nil
}

func currentReplicaSetConfig(session mongoRunner) (*replicaSetConfig, error) {
	var result struct {
		Config replicaSetConfig `bson:"config"`
	}
	if err := session.Run(bson.D{{Name: "replSetGetConfig", Value: 1}}, &result); err != nil {
		return nil, fmt.Errorf("running replSetGetConfig: %w", err)
	}
	return &result.Config, nil
}

func replSetMembers(session mongoRunner) ([]member, error) {
	status, err := currentReplicaSetStatus(session)
	if err != nil {
		return nil, err
	}
	config, err := currentReplicaSetConfig(session)
	if err != nil {
		return nil, err
	}
	byID := map[int]member{}
	for _, m := range status.Members {
		byID[m.ID] = member{ID: m.ID, Address: m.Address, State: m.State, Health: m.Health}
	}
	var out []member
	for _, c := range config.Members {
		m := byID[c.ID]
		m.ID = c.ID
		m.Address = c.Address
		m.Votes = memberVotes(c)
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

func removedMemberForMachine(session *mgo.Session, modelUUID, machine string, nodes []dqlite.NodeInfo, statePort int) (member, bool, error) {
	var doc machineNetworkDoc
	if err := session.DB(stateDB).C("machines").Find(bson.M{"machineid": machine, "model-uuid": modelUUID}).One(&doc); err != nil {
		return member{}, false, fmt.Errorf("reading machine %s addresses: %w", machine, err)
	}
	address, dqliteAlreadyRemoved, err := removedMemberAddress(&doc, nodes, statePort)
	if err != nil {
		return member{}, false, err
	}
	return member{Address: address, MachineID: machine}, dqliteAlreadyRemoved, nil
}

func removedMemberAddress(doc *machineNetworkDoc, nodes []dqlite.NodeInfo, statePort int) (string, bool, error) {
	hosts := map[string]bool{}
	for _, address := range append(doc.Addresses, doc.MachineAddresses...) {
		if address.Scope != "local-machine" && address.Value != "" {
			hosts[address.Value] = true
		}
	}
	var matches []string
	for _, node := range nodes {
		host, _, err := net.SplitHostPort(node.Address)
		if err != nil {
			return "", false, fmt.Errorf("parsing Dqlite node address %q: %w", node.Address, err)
		}
		if hosts[host] {
			matches = append(matches, host)
		}
	}
	if len(matches) == 0 {
		return "", true, nil
	}
	if len(matches) > 1 {
		return "", false, fmt.Errorf("machine addresses match %d Dqlite nodes, need at most one", len(matches))
	}
	return net.JoinHostPort(matches[0], fmt.Sprint(statePort)), false, nil
}

// confirmDead re-reads the replica set status a few times. The target must be
// unhealthy and DOWN or UNKNOWN in every sample, so a member that is merely
// slow or briefly partitioned is never treated as dead.
func confirmDead(session mongoRunner, machine string) error {
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
		if m.Health != 0 || (m.State != downState && m.State != unknownState) {
			return fmt.Errorf("member %s is not dead (state=%d health=%v); refusing to evict", m.Address, m.State, m.Health)
		}
	}
	return nil
}

func memberVotes(member replicaSetConfigMember) int {
	if member.Votes == nil {
		return 1
	}
	return *member.Votes
}

func canBecomePrimary(member replicaSetConfigMember) bool {
	if memberVotes(member) == 0 || (member.Arbiter != nil && *member.Arbiter) {
		return false
	}
	return member.Priority == nil || *member.Priority > 0
}

func planForcedReplicaSetEviction(session mongoRunner, target member) (*replicaSetEviction, error) {
	config, err := currentReplicaSetConfig(session)
	if err != nil {
		return nil, err
	}
	found := false
	for _, configured := range config.Members {
		if configured.ID != target.ID {
			continue
		}
		found = configured.Address == target.Address
		if !found {
			return nil, fmt.Errorf("replica set member %d address changed from %s to %s", target.ID, target.Address, configured.Address)
		}
		if memberVotes(configured) == 0 {
			return nil, fmt.Errorf("replica set member %d is no longer a voter; retry the command", target.ID)
		}
		break
	}
	if !found {
		return nil, fmt.Errorf("replica set member %d (%s) is no longer configured; retry the command", target.ID, target.Address)
	}

	samples := make([]replicaSetStatus, 0, deadSamples)
	for i := 0; i < deadSamples; i++ {
		if i > 0 {
			time.Sleep(deadInterval)
		}
		status, err := currentReplicaSetStatus(session)
		if err != nil {
			return nil, fmt.Errorf("assessing replica set eviction: %w", err)
		}
		samples = append(samples, *status)
	}
	if err := assessForcedReplicaSetEviction(config, target.ID, samples); err != nil {
		return nil, err
	}
	return &replicaSetEviction{
		MemberID:      target.ID,
		MemberAddress: target.Address,
		NoPrimary:     replicaSetSamplesHaveNoPrimary(samples),
		Config:        *config,
	}, nil
}

func replicaSetSamplesHaveNoPrimary(samples []replicaSetStatus) bool {
	for _, status := range samples {
		for _, member := range status.Members {
			if member.State == primaryState && member.Health != 0 {
				return false
			}
		}
	}
	return true
}

func assessForcedReplicaSetEviction(config *replicaSetConfig, targetID int, samples []replicaSetStatus) error {
	if len(samples) != deadSamples {
		return fmt.Errorf("replica set eviction requires %d status samples, got %d", deadSamples, len(samples))
	}
	voting := map[int]bool{}
	primaryEligible := map[int]bool{}
	targetConfigured := false
	for _, member := range config.Members {
		if member.ID == targetID {
			targetConfigured = true
		}
		if memberVotes(member) > 0 {
			voting[member.ID] = true
		}
		if canBecomePrimary(member) {
			primaryEligible[member.ID] = true
		}
	}
	if !targetConfigured {
		return fmt.Errorf("replica set member %d is not configured", targetID)
	}

	targetDead := true
	var liveVoting, livePrimaryEligible, unhealthyOtherVoting map[int]bool
	for i, status := range samples {
		deadNow := false
		liveVotingNow := map[int]bool{}
		livePrimaryEligibleNow := map[int]bool{}
		unhealthyOtherVotingNow := map[int]bool{}
		for _, member := range status.Members {
			if member.Health != 0 {
				if voting[member.ID] {
					liveVotingNow[member.ID] = true
				}
				if primaryEligible[member.ID] && (member.State == primaryState || member.State == secondaryState) {
					livePrimaryEligibleNow[member.ID] = true
				}
				continue
			}
			if member.ID == targetID && (member.State == downState || member.State == unknownState) {
				deadNow = true
			}
			if voting[member.ID] && member.ID != targetID {
				unhealthyOtherVotingNow[member.ID] = true
			}
		}
		targetDead = targetDead && deadNow
		if i == 0 {
			liveVoting = liveVotingNow
			livePrimaryEligible = livePrimaryEligibleNow
			unhealthyOtherVoting = unhealthyOtherVotingNow
			continue
		}
		liveVoting = intersectIDs(liveVoting, liveVotingNow)
		livePrimaryEligible = intersectIDs(livePrimaryEligible, livePrimaryEligibleNow)
		unhealthyOtherVoting = intersectIDs(unhealthyOtherVoting, unhealthyOtherVotingNow)
	}

	if !targetDead {
		return fmt.Errorf("replica set member %d did not remain DOWN or UNKNOWN across all samples", targetID)
	}
	if len(unhealthyOtherVoting) > 0 {
		return fmt.Errorf("voters %v are unhealthy but not removal targets", sortedIDs(unhealthyOtherVoting))
	}

	remainingVoters := 0
	for id := range voting {
		if id != targetID {
			remainingVoters++
		}
	}
	liveVoters := 0
	for id := range liveVoting {
		if id != targetID {
			liveVoters++
		}
	}
	needed := (remainingVoters / 2) + 1
	if liveVoters < needed {
		return fmt.Errorf("evicting member %d leaves %d live voters of %d, but a majority needs %d", targetID, liveVoters, remainingVoters, needed)
	}
	if len(livePrimaryEligible) == 0 {
		return fmt.Errorf("evicting member %d leaves no live primary-eligible member", targetID)
	}
	return nil
}

func intersectIDs(left, right map[int]bool) map[int]bool {
	intersection := map[int]bool{}
	for id := range left {
		if right[id] {
			intersection[id] = true
		}
	}
	return intersection
}

func sortedIDs(ids map[int]bool) []int {
	values := make([]int, 0, len(ids))
	for id := range ids {
		values = append(values, id)
	}
	sort.Ints(values)
	return values
}

func revalidateReplicaSetEviction(session mongoRunner, planned *replicaSetEviction) error {
	fresh, err := planForcedReplicaSetEviction(session, member{ID: planned.MemberID, Address: planned.MemberAddress})
	if err != nil {
		return fmt.Errorf("revalidating forced replica set eviction: %w", err)
	}
	if !reflect.DeepEqual(planned, fresh) {
		return fmt.Errorf("replica set config changed after planning; no changes were applied, retry the command")
	}
	return nil
}

func reconfigureReplicaSetWithoutMember(session mongoRunner, eviction *replicaSetEviction, force bool) error {
	config := eviction.Config
	config.Version++
	config.Members = make([]replicaSetConfigMember, 0, len(eviction.Config.Members)-1)
	found := false
	for _, member := range eviction.Config.Members {
		if member.ID == eviction.MemberID {
			found = true
			continue
		}
		config.Members = append(config.Members, member)
	}
	if !found {
		return fmt.Errorf("replica set member %d is missing from the planned config", eviction.MemberID)
	}

	command := bson.D{{Name: "replSetReconfig", Value: config}}
	if force {
		command = append(command, bson.DocElem{Name: "force", Value: true})
	}
	err := session.Run(command, nil)
	if err == io.EOF {
		session.Refresh()
	} else if err != nil {
		return fmt.Errorf("removing replica set member %d: %w", eviction.MemberID, err)
	}
	var lastStatusErr error
	for i := 0; i < primaryPollAttempts; i++ {
		status, err := currentReplicaSetStatus(session)
		if err != nil {
			lastStatusErr = err
			session.Refresh()
		} else {
			for _, member := range status.Members {
				if member.State == primaryState {
					return nil
				}
			}
		}
		if i+1 < primaryPollAttempts {
			time.Sleep(primaryPollInterval)
		}
	}
	if lastStatusErr != nil {
		return fmt.Errorf("no MongoDB primary elected after removing member %d; last status check failed: %w", eviction.MemberID, lastStatusErr)
	}
	return fmt.Errorf("no MongoDB primary elected after removing member %d", eviction.MemberID)
}

func isQuorumCheckFailure(err error) bool {
	// Code 11602 also identifies a primary transition, so it is not enough to
	// permit a forced reconfig without MongoDB's explicit quorum-check message.
	var queryError *mgo.QueryError
	return errors.As(err, &queryError) && queryError != nil &&
		strings.Contains(queryError.Message, "Quorum check failed")
}

func planMongo(session *mgo.Session, modelUUID, machine string, p *plan) error {
	db := session.DB(stateDB)
	appDecrements := map[string]int{}
	if p.ModelUUID == "" {
		p.ModelUUID = modelUUID
	} else if p.ModelUUID != modelUUID {
		return fmt.Errorf("plan model %s does not match controller model %s", p.ModelUUID, modelUUID)
	}
	p.MachineDocID = p.ModelUUID + ":" + machine
	if err := db.C("machines").FindId(p.MachineDocID).One(&p.MachineDoc); err != nil {
		return fmt.Errorf("reading machine %s doc: %w", machine, err)
	}
	if err := assertNoPendingTxn(p.MachineDoc, "machines", p.MachineDocID); err != nil {
		return err
	}

	var units []map[string]interface{}
	if err := db.C("units").Find(bson.M{"machineid": machine, "model-uuid": modelUUID}).All(&units); err != nil {
		return fmt.Errorf("finding units on machine %s: %w", machine, err)
	}
	if len(units) == 0 {
		principals, err := machinePrincipals(p.MachineDoc)
		if err != nil {
			return fmt.Errorf("machines/%s: %w", p.MachineDocID, err)
		}
		if len(principals) > 0 {
			return fmt.Errorf("no unit documents reference machine %s but the machine still has principals", machine)
		}
		return nil
	}

	for _, u := range units {
		id, ok := u["_id"].(string)
		if !ok || id == "" {
			return fmt.Errorf("unit document on machine %s has no string _id", machine)
		}
		name, ok := u["name"].(string)
		if !ok || name == "" {
			return fmt.Errorf("units/%s has no string name", id)
		}
		parts := strings.SplitN(name, "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			return fmt.Errorf("units/%s has invalid unit name %q", id, name)
		}
		if err := assertNoPendingTxn(u, "units", id); err != nil {
			return err
		}
		unitModelUUID, ok := u["model-uuid"].(string)
		if !ok || unitModelUUID == "" {
			return fmt.Errorf("units/%s has no string model-uuid", id)
		}
		if unitModelUUID != p.ModelUUID {
			return fmt.Errorf("units/%s belongs to model %s, expected %s", id, unitModelUUID, p.ModelUUID)
		}
		p.Units = append(p.Units, name)
		p.Delete = append(p.Delete, deletion{Collection: "units", ID: id, Doc: u})
		appDecrements[parts[0]]++

		re := unitDocRegexp(name)
		for _, coll := range unitDocCollections {
			var docs []map[string]interface{}
			if err := db.C(coll).Find(bson.M{"_id": bson.M{"$regex": re}, "model-uuid": modelUUID}).All(&docs); err != nil {
				return fmt.Errorf("finding %s documents for %s: %w", coll, name, err)
			}
			for _, d := range docs {
				did, ok := d["_id"].(string)
				if !ok || did == "" {
					return fmt.Errorf("%s document for %s has no string _id", coll, name)
				}
				if err := assertNoPendingTxn(d, coll, did); err != nil {
					return err
				}
				p.Delete = append(p.Delete, deletion{Collection: coll, ID: did, Doc: d})
			}
		}
	}
	sort.Strings(p.Units)
	sort.Slice(p.Delete, func(i, j int) bool {
		if p.Delete[i].Collection == p.Delete[j].Collection {
			return p.Delete[i].ID < p.Delete[j].ID
		}
		return p.Delete[i].Collection < p.Delete[j].Collection
	})

	appNames := make([]string, 0, len(appDecrements))
	for name := range appDecrements {
		appNames = append(appNames, name)
	}
	sort.Strings(appNames)
	for _, name := range appNames {
		id := p.ModelUUID + ":" + name
		var doc map[string]interface{}
		if err := db.C("applications").FindId(id).One(&doc); err != nil {
			return fmt.Errorf("reading application %s doc: %w", name, err)
		}
		change, err := applicationChangeFor(name, id, appDecrements[name], doc)
		if err != nil {
			return err
		}
		p.Applications = append(p.Applications, change)
	}

	if err := validateMachinePrincipals(p.MachineDoc, p.Units); err != nil {
		return fmt.Errorf("machines/%s: %w", p.MachineDocID, err)
	}
	return nil
}

func applicationChangeFor(name, id string, decrement int, doc map[string]interface{}) (applicationChange, error) {
	if err := assertNoPendingTxn(doc, "applications", id); err != nil {
		return applicationChange{}, err
	}
	unitCount, ok := doc["unitcount"].(int)
	if !ok {
		return applicationChange{}, fmt.Errorf("applications/%s unitcount is missing or not an integer", id)
	}
	if unitCount < decrement {
		return applicationChange{}, fmt.Errorf("applications/%s unitcount is %d; cannot decrement by %d", id, unitCount, decrement)
	}
	return applicationChange{
		Name:            name,
		ID:              id,
		UnitCountBefore: unitCount,
		Decrement:       decrement,
		Doc:             doc,
	}, nil
}

func validateMachinePrincipals(doc map[string]interface{}, units []string) error {
	principals, err := machinePrincipals(doc)
	if err != nil {
		return err
	}
	for _, unit := range units {
		if !principals[unit] {
			return fmt.Errorf("unit %s is not present in principals", unit)
		}
	}
	return nil
}

func machinePrincipals(doc map[string]interface{}) (map[string]bool, error) {
	raw, ok := doc["principals"]
	if !ok {
		return nil, fmt.Errorf("principals is missing")
	}
	principals := map[string]bool{}
	switch values := raw.(type) {
	case []interface{}:
		for _, value := range values {
			principal, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("principals contains a non-string value")
			}
			principals[principal] = true
		}
	case []string:
		for _, principal := range values {
			principals[principal] = true
		}
	default:
		return nil, fmt.Errorf("principals is not an array")
	}
	return principals, nil
}

func revalidateMongoPlan(session *mgo.Session, planned *plan) error {
	fresh := plan{Machine: planned.Machine, ModelUUID: planned.ModelUUID}
	if err := planMongo(session, planned.ModelUUID, planned.Machine, &fresh); err != nil {
		return fmt.Errorf("revalidating Mongo plan before write: %w", err)
	}
	if !sameMongoPlan(planned, &fresh) {
		return fmt.Errorf("Mongo state changed after planning; no changes were applied, retry the command")
	}
	return nil
}

func sameMongoPlan(a, b *plan) bool {
	return a.Machine == b.Machine &&
		a.ModelUUID == b.ModelUUID &&
		a.MachineDocID == b.MachineDocID &&
		reflect.DeepEqual(a.Units, b.Units) &&
		reflect.DeepEqual(a.Delete, b.Delete) &&
		reflect.DeepEqual(a.Applications, b.Applications) &&
		reflect.DeepEqual(a.MachineDoc, b.MachineDoc)
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
	ops, err := mongoCleanupOps(p)
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	if err := sstxn.NewRunner(db, nil).Run(ops, "", nil); err != nil {
		return fmt.Errorf("applying Mongo cleanup transaction: %w", err)
	}
	return nil
}

func mongoCleanupOps(p *plan) ([]txn.Op, error) {
	// sstxn sets txn-revno to its previous value plus one for every update.
	// Including it in an update here would modify the same field twice.
	ops := make([]txn.Op, 0, len(p.Delete)+len(p.Applications)+1)
	for _, d := range p.Delete {
		assert, err := txnRevnoAssertion(d.Doc, d.Collection, d.ID)
		if err != nil {
			return nil, err
		}
		ops = append(ops, txn.Op{C: d.Collection, Id: d.ID, Assert: assert, Remove: true})
	}
	for _, app := range p.Applications {
		assert, err := txnRevnoAssertion(app.Doc, "applications", app.ID)
		if err != nil {
			return nil, err
		}
		assert = append(assert, bson.DocElem{Name: "unitcount", Value: app.UnitCountBefore})
		ops = append(ops, txn.Op{
			C:      "applications",
			Id:     app.ID,
			Assert: assert,
			Update: bson.M{"$inc": bson.M{"unitcount": -app.Decrement}},
		})
	}
	if len(p.Units) > 0 {
		assert, err := txnRevnoAssertion(p.MachineDoc, "machines", p.MachineDocID)
		if err != nil {
			return nil, err
		}
		ops = append(ops, txn.Op{
			C:      "machines",
			Id:     p.MachineDocID,
			Assert: assert,
			Update: bson.M{"$pullAll": bson.M{"principals": p.Units}},
		})
	}
	return ops, nil
}

func txnRevnoAssertion(doc map[string]interface{}, collection, id string) (bson.D, error) {
	revno, ok := doc["txn-revno"]
	if !ok {
		return nil, fmt.Errorf("%s/%s has no txn-revno", collection, id)
	}
	return bson.D{{Name: "txn-revno", Value: revno}}, nil
}

// advanceMachineDead waits for Juju's own cleanup to advance the machine from
// Alive to Dying and remove its controller reference. It then sets the machine
// Dead so the live provisioner removes it and its related documents with proper
// transactions. It never performs that heavy removal itself.
func advanceMachineDead(session *mgo.Session, p *plan) error {
	db := session.DB(stateDB)
	deadline := time.Now().Add(dyingWait)
	for {
		controllerRegistered, err := controllerReferencePresent(db, p.Machine)
		if err != nil {
			return err
		}
		var m struct {
			Life int `bson:"life"`
		}
		err = db.C("machines").FindId(p.MachineDocID).One(&m)
		if err == mgo.ErrNotFound {
			if controllerRegistered {
				return fmt.Errorf("machine %s was removed while its controller reference still exists", p.Machine)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading machine life: %w", err)
		}
		switch m.Life {
		case lifeDying:
			if controllerRegistered {
				break
			}
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
			if controllerRegistered {
				return fmt.Errorf("machine %s is Dead but its controller reference still exists", p.Machine)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("machine %s did not finish controller cleanup within %s; is 'juju remove-machine --force' pending?", p.Machine, dyingWait)
		}
		time.Sleep(dyingPollTick)
	}
}

func controllerReferencePresent(db *mgo.Database, machine string) (bool, error) {
	var doc struct {
		ControllerIDs []string `bson:"controller-ids"`
	}
	if err := db.C(controllersC).FindId(modelGlobalKey).One(&doc); err != nil {
		return false, fmt.Errorf("reading controller references: %w", err)
	}
	for _, id := range doc.ControllerIDs {
		if id == machine {
			return true, nil
		}
	}
	return false, nil
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

func printPlan(w io.Writer, p *plan) {
	if p.ReplicaSetEviction != nil {
		action := "remove"
		reason := "force only if the normal reconfig loses quorum"
		if p.ReplicaSetEviction.NoPrimary {
			action = "force-remove"
			reason = "no primary is available"
		}
		fmt.Fprintf(w, "  mongo: %s replica set member %d (%s), config version %d -> %d; %s\n",
			action,
			p.ReplicaSetEviction.MemberID,
			p.ReplicaSetEviction.MemberAddress,
			p.ReplicaSetEviction.Config.Version,
			p.ReplicaSetEviction.Config.Version+1,
			reason,
		)
	}
	for _, d := range p.Delete {
		fmt.Fprintf(w, "  mongo: delete %s/%s\n", d.Collection, d.ID)
	}
	for _, app := range p.Applications {
		fmt.Fprintf(w, "  mongo: update applications/%s unitcount %d -> %d\n", app.ID, app.UnitCountBefore, app.UnitCountAfter())
	}
	if len(p.Units) > 0 {
		fmt.Fprintf(w, "  mongo: update machines/%s remove principals %s\n", p.MachineDocID, strings.Join(p.Units, ", "))
	}
	if p.DqliteNodeID != 0 {
		fmt.Fprintf(w, "  dqlite: remove node %d (%s)\n", p.DqliteNodeID, p.DqliteAddress)
	}
}
