# juju-controller-evict

Remove a permanently dead Juju HA controller from a live controller cluster,
without bringing the dead machine back.

## The problem

A Juju HA controller runs three independent clusters at once.
The MongoDB replica set holds model state.
The Dqlite cluster holds controller state in Juju 3.6.
The controller charm has its own leadership.

When a controller machine is powered off for good and you run
`juju remove-machine <id> -m controller --force`, the command returns but the
machine never leaves.
Two things block it.

1. The `evacuateMachine` cleanup waits for the dead machine agent to tear down
   its controller unit. `juju remove-unit` does not work either, because Juju
   refuses to remove units of the controller application.
2. A Dqlite node is removed only by the departing node's own handover. The
   surviving nodes never evict it. So the dead node stays a cluster member.

The result is a machine stuck in `down`, a stale MongoDB member, and a stale
Dqlite node that never go away on their own.

## What this tool does

It runs on a surviving controller and makes the smallest change needed, then
lets Juju's own workers finish the removal with normal transactions.

For MongoDB, it deletes the dead machine's unit documents and pulls the unit
from the machine's principals. The live cleanup worker then advances the
machine to `Dying`, which also removes the controller reference and the dead
replica set member. The tool then sets the machine `Dead`, and the live
provisioner removes the machine and all of its related documents. The tool does
not rebuild the machine removal by hand.

For Dqlite, it connects to the cluster leader and removes the dead node.

## Preconditions

- The dead controller is never coming back. Do not use this on a machine you
  can boot again. If you can boot it, just start it and `juju remove-machine`
  works on its own.
- You already ran `juju remove-machine <id> -m controller --force`. That
  schedules the cleanup this tool unblocks.
- The controller still has quorum and answers `juju status`.
- The dead member reports `DOWN` in the MongoDB replica set. The tool checks
  this three times before it acts.

## Usage

From a Juju client, as an admin. The tool copies itself to a surviving
controller over `juju scp`, runs there, and cleans up after itself.

    # report only, change nothing
    juju-controller-evict -controller mycontroller

    # dry run for machine 1
    juju-controller-evict -controller mycontroller -machine 1

    # apply
    juju-controller-evict -controller mycontroller -machine 1 -yes

You can also run it directly on a surviving controller machine as root. In that
mode it reads the local `agent.conf` and does not use `juju scp`.

    sudo ./juju-controller-evict -machine 1 -yes

After it finishes, watch `juju status` until the machine disappears, then run
`juju enable-ha` to restore three voters.

### Flags

- `-machine` machine id of the dead controller. Omit to only print cluster state.
- `-controller` controller name when run from a client. Default is the current
  controller.
- `-yes` apply the changes. Without it the tool only reports and writes a backup.
- `-skip-mongo` leave the Juju state documents alone. This makes it a Dqlite
  only tool.
- `-skip-dqlite` leave the Dqlite cluster alone.
- `-backup` path for the JSON backup of every document it deletes or changes.

## Safety

This tool edits the controller state database directly. Use it only for a
controller that is gone for good.

- It does nothing without `-yes`.
- It writes a JSON backup of every document before it changes anything.
- It refuses to touch the current Dqlite leader, or a node that still answers on
  its Dqlite port.
- It only acts on a MongoDB member that is unhealthy and `DOWN` across several
  samples, so a short network blip cannot look like a dead node.
- It refuses to run against the machine it is running on.

## Limitations

- Tested with Juju 3.6 (MongoDB replica set plus Dqlite). Older or newer Juju
  may store state differently.
- It removes one dead controller at a time.
- After removal the Dqlite cluster has fewer voters. Run `juju enable-ha` to
  restore the voter count.

## Build

Static binary, no cgo. It links only the pure Go Dqlite client, not the C
backed Dqlite app.

    CGO_ENABLED=0 go build -o juju-controller-evict .

Copy the binary to the client or controller and run it.

## License

Apache-2.0. See LICENSE.
