# Migrating from v7 to v7.1

This guide provides instructions for migrating to version `v7.1.0` of ibc-go.

There are four sections based on the four potential user groups of this document:

- [Migrating from v7 to v7.1](#migrating-from-v7-to-v71)
  - [Chains](#chains)
  - [IBC Apps](#ibc-apps)
  - [Relayers](#relayers)
  - [IBC Light Clients](#ibc-light-clients)

**Note:** ibc-go supports golang semantic versioning and therefore all imports must be updated on major version releases.

## Chains

### 09-localhost migration

In the previous release of ibc-go, the localhost `v1` light client module was deprecated and removed. The ibc-go `v7.1.0` release introduces `v2` of the 09-localhost light client module.

<!-- TODO: Update the links to use release version instead of feat branch -->
An [automatic migration handler](https://github.com/cosmos/ibc-go/blob/release/v7.1.x/modules/core/module.go#L127-L145) is configured in the core IBC module to set the localhost `ClientState` and sentinel `ConnectionEnd` in state.

In order to use the 09-localhost client chains must update the `AllowedClients` parameter in the 02-client submodule of core IBC. This can be configured directly in the application upgrade handler or alternatively updated via the legacy governance parameter change proposal.
We __strongly__ recommend chains to perform this action so that intra-ledger communication can be carried out using the familiar IBC interfaces.

See the upgrade handler code sample provided below or [follow this link](https://github.com/cosmos/ibc-go/blob/release/v7.1.x/testing/simapp/upgrades/upgrades.go#L85) for the upgrade handler used by the ibc-go simapp.

```go
func CreateV7LocalhostUpgradeHandler(
  mm *module.Manager,
  configurator module.Configurator,
  clientKeeper clientkeeper.Keeper,
) upgradetypes.UpgradeHandler {
  return func(ctx sdk.Context, _ upgradetypes.Plan, vm module.VersionMap) (module.VersionMap, error) {
    // explicitly update the IBC 02-client params, adding the localhost client type
    params := clientKeeper.GetParams(ctx)
    params.AllowedClients = append(params.AllowedClients, exported.Localhost)
    clientKeeper.SetParams(ctx, params)

    return mm.RunMigrations(ctx, configurator, vm)
  }
}
```

### Transfer migration

An automatic migration handler (TODO: add link after backport to v7.1.x is merged) is configured in the transfer module to set the total amount in escrow for all denominations of coins that have been sent out. For each denomination a state entry is added with the total amount of coins in escrow regardless of the channel from which they were transferred.

## IBC Apps

- No relevant changes were made in this release.

## Relayers

The event attribute `packet_connection` (`connectiontypes.AttributeKeyConnection`) has been deprecated. 
Please use the `connection_id` attribute (`connectiontypes.AttributeKeyConnectionID`) which is emitted by all channel events.
Only send packet, receive packet, write acknowledgement, and acknowledge packet events used `packet_connection` previously.

## IBC Light Clients

- No relevant changes were made in this release.
