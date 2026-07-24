# juju-controller-evict

`juju-controller-evict` removes a permanently dead Juju 3.6 HA controller from a live controller cluster without starting the dead machine again.

This is a recovery tool for affected Juju 3.6 controllers. It edits Juju state in MongoDB and removes a Dqlite member. Use it only when the controller machine is permanently unavailable.

## When to use it

Use this tool only when all of these conditions are true:

- The controller machine will not return.
- You already ran `juju remove-machine <id> -m <controller>:controller --force`.
- At least one controller is still running.
- The dead MongoDB member reports `DOWN` or `UNKNOWN` in repeated checks.

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

Run it directly on a surviving controller when MongoDB has no primary. Client mode depends on the Juju API, which may be unavailable after MongoDB loses quorum.

## What it changes

If the dead MongoDB member still has a vote and blocks the peer grouper, the tool first tries a normal replica-set reconfig. It uses a forced reconfig only when MongoDB rejects the normal attempt with a quorum-check failure. If no primary exists, the tool connects directly to the local MongoDB member and plans a forced reconfig. Every status sample must show that no primary exists. The tool samples the replica-set status again before any forced attempt. The remaining healthy voters must retain a majority and include a member that can become primary.

If the replica-set change succeeds but the command stops before Juju cleanup, run the same command again. The tool matches the machine addresses to the remaining Dqlite node and continues after the MongoDB member has gone.

The tool then removes the dead controller unit documents that block Juju cleanup. Juju removes the controller reference, and the tool marks the machine `Dead` so the provisioner can finish removing it.

It also removes the matching Dqlite node from the cluster.

Before changing MongoDB, the tool writes the original replica-set config when applicable, the selected unit documents, the original machine document, and the application documents to a JSON file. The controller-side file is created with permissions restricted to its owner. In client mode, it is copied to the path passed with `-backup`, which defaults to `juju-controller-evict-backup.json`.

## Safety checks

The tool refuses to apply changes unless:

- `-yes` is present.
- The target is not the controller running the tool.
- The MongoDB member is unhealthy and remains `DOWN` or `UNKNOWN` across three checks.
- A direct MongoDB connection is used only when every check reports no primary.
- A forced replica-set reconfig has no persistently unhealthy voter outside the removal target.
- The remaining healthy voters can form a majority and include a primary-eligible member.
- The target Dqlite node is not the current leader.
- The target no longer answers on its Dqlite port.

This tool changes controller state directly. The JSON file is a record of the affected MongoDB state, not an automatic rollback.

## After removal

Watch `juju status` until the machine disappears. Then restore the controller voter count:

```text
juju enable-ha -c mycontroller
```

## Options

- `-controller` selects the controller in client mode. It defaults to the current controller.
- `-machine` selects the dead controller machine. Omit it to only report cluster state.
- `-yes` applies the plan. Without it, the tool only reports the plan.
- `-backup` selects the JSON backup path. The default is `juju-controller-evict-backup.json`.
- `-timeout` sets the timeout shared by Dqlite calls. The default is two minutes.
- `-version` prints the build version.

Run `juju-controller-evict -help` for the exact option syntax.

## Limitations

- Tested with Juju 3.6 only.
- Removes one dead controller at a time.
- Requires a live primary-eligible MongoDB member and a Dqlite leader on the surviving controllers.

## Validation on a disposable controller

Do not run these tests on a production controller. The tests remove controller machines and may force a MongoDB replica-set reconfig.

Test the normal reconfig path with three voting controllers:

1. Stop one controller machine at the provider.
2. Run `juju remove-machine <id> -m <controller>:controller --force --no-prompt` while the other two controllers still have quorum.
3. Run `juju-controller-evict -controller <controller> -machine <id>`. An immediate run may refuse while the member is still transitioning. Wait until all samples report it down, then check that the plan says `remove replica set member`.
4. Re-run with `-yes`.
5. Check that `juju status -m <controller>:controller` no longer lists the machine.
6. Run the tool without `-machine` and check that MongoDB and Dqlite no longer list the member.
7. Run `juju enable-ha -c <controller>` and check that three controllers become available again.

Test the no-primary force path from a disposable fixture snapshot that has exactly two voting MongoDB members and a pending forced machine-removal request. This topology is only for exercising the recovery path. Do not create it on a controller that holds useful models.

1. Stop one voting controller so the survivor becomes `SECONDARY` and the stopped member becomes `DOWN`.
2. Copy the binary to the surviving controller through the provider. The Juju API may be unavailable.
3. Run `sudo ./juju-controller-evict -machine <id>` on the survivor.
4. Check that the plan says `force-remove replica set member` and `no primary is available`.
5. Re-run with `-yes` and keep the JSON backup.
6. Check that the survivor becomes `PRIMARY` and that MongoDB and Dqlite each contain only the survivor.
7. Check that the removed machine is absent from `juju status -m <controller>:controller`.
8. Run `juju enable-ha -c <controller>` and check that three controllers become available again.

Do not reproduce every rejection case by damaging a cluster. Run `go test ./...`. The unit tests exercise sampling decisions for a recovering target, another unhealthy voter, an unstable majority, no primary-eligible member, and a primary that appears in one sample. They also check that authentication and TLS errors are not mistaken for a topology failure, reject ambiguous Dqlite addresses, and reject a cleanup plan when its captured state changes.

## Build

```text
CGO_ENABLED=0 go build -o juju-controller-evict .
```

## License

Apache-2.0. See [LICENSE](LICENSE).
