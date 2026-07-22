# juju-controller-evict

`juju-controller-evict` removes a permanently dead Juju 3.6 HA controller from a live controller cluster without starting the dead machine again.

This is a recovery tool for affected Juju 3.6 controllers. It edits Juju state in MongoDB and removes a Dqlite member. Use it only when the controller machine is permanently unavailable.

## When to use it

Use this tool only when all of these conditions are true:

- The controller machine will not return.
- You already ran `juju remove-machine <id> -m controller --force`.
- The remaining controllers have quorum and answer `juju status`.
- The dead MongoDB member reports `DOWN` and has no vote.

If the machine can be recovered, start it and let Juju remove it normally.

## Usage

Run the binary from a Juju client logged in as a controller administrator.

```text
# Show the MongoDB and Dqlite members.
juju-controller-evict -controller mycontroller

# Check the removal plan for machine 1 without changing anything.
juju-controller-evict -controller mycontroller -machine 1

# Apply the plan.
juju-controller-evict -controller mycontroller -machine 1 -yes
```

Without `-yes`, the tool does not change MongoDB or Dqlite.

The client copies the binary to a surviving controller and runs it there with `sudo`. The binary must be built for the controller architecture, usually Linux `amd64` or `arm64`.

You can also run it directly on a surviving controller:

```text
sudo ./juju-controller-evict -machine 1 -yes
```

## What it changes

The tool removes the dead controller unit documents that block Juju cleanup. Juju then removes the controller reference and MongoDB replica-set member. The tool marks the machine `Dead` so the provisioner can finish removing it.

It also removes the matching Dqlite node from the cluster.

Before changing MongoDB, the tool writes the selected unit documents, the original machine document, and the planned application unit-count changes to a JSON file. Client mode copies this file back as `juju-controller-evict-backup-<machine>.json`.

## Safety checks

The tool refuses to apply changes unless:

- `-yes` is present.
- The target is not the controller running the tool.
- The MongoDB member is unhealthy and remains `DOWN` across three checks.
- The MongoDB member is already non-voting.
- The target Dqlite node is not the current leader.
- The target no longer answers on its Dqlite port.

This tool changes controller state directly. The JSON file is a record of the affected MongoDB state, not an automatic rollback.

## After removal

Watch `juju status` until the machine disappears. Then restore the controller voter count:

```text
juju enable-ha
```

## Other options

- `-skip-mongo` removes only the Dqlite node.
- `-skip-dqlite` changes only the Juju state in MongoDB.
- `-agent-conf` selects the controller agent configuration and forces direct controller mode.
- `-backup` changes the JSON output path.
- `-timeout` sets the Dqlite operation timeout.

Run `juju-controller-evict -help` for all path and connection options.

## Limitations

- Tested with Juju 3.6 only.
- Removes one dead controller at a time.
- Requires a MongoDB primary and a Dqlite leader on the surviving controllers.

## Build

```text
CGO_ENABLED=0 go build -o juju-controller-evict .
```

## License

Apache-2.0. See [LICENSE](LICENSE).
