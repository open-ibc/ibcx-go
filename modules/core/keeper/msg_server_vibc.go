package keeper

import (
	"context"

	metrics "github.com/armon/go-metrics"
	"github.com/cosmos/cosmos-sdk/telemetry"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"

	channeltypes "github.com/cosmos/ibc-go/v7/modules/core/04-channel/types"
	host "github.com/cosmos/ibc-go/v7/modules/core/24-host"
	coretypes "github.com/cosmos/ibc-go/v7/modules/core/types"
	vibctypes "github.com/cosmos/ibc-go/v7/modules/core/vibc/types"
)

var _ vibctypes.VirtualIBCSidecarServer = Keeper{}

func (k Keeper) PostChannelOpenInitOrTry(goCtx context.Context, msg *vibctypes.MsgPostChannelOpenInitOrTry) (*vibctypes.MsgPostChannelOpenInitOrTryResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	// check following:
	// - channel sequence for channel_id == last sequence
	// - non-exists: channel<INIT/TRYOPEN>
	channelSequence, err := channeltypes.ParseChannelSequence(msg.ChannelId)
	if err != nil {
		return nil, err
	}
	if channelSequence+1 != k.ChannelKeeper.GetNextChannelSequence(ctx) {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelIdentifier, "expecting channel sequence == last sequence")
	}

	_, found := k.ChannelKeeper.GetChannel(ctx, msg.PortId, msg.ChannelId)
	if found {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannel, "already exists")
	}

	switch msg.Channel.State {
	case channeltypes.INIT:
		k.ChannelKeeper.WriteOpenInitChannel(ctx, msg.PortId, msg.ChannelId, msg.Channel.Ordering, msg.Channel.ConnectionHops,
			msg.Channel.Counterparty, msg.Channel.Version)
	case channeltypes.TRYOPEN:
		// TODO: Verify INIT channel on counterparty
		k.ChannelKeeper.WriteOpenTryChannel(ctx, msg.PortId, msg.ChannelId, msg.Channel.Ordering, msg.Channel.ConnectionHops,
			msg.Channel.Counterparty, msg.Channel.Version)
	default:
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelState, "expecting INIT or TRYOPEN")
	}

	ctx.Logger().Info("post channel open init or try callback succeeded", "channel-id", msg.ChannelId, "port-id", msg.PortId, "version", msg.Channel.Version)

	return &vibctypes.MsgPostChannelOpenInitOrTryResponse{}, nil
}

func (k Keeper) PostChannelOpenAck(goCtx context.Context, msg *vibctypes.MsgPostChannelOpenAck) (*vibctypes.MsgPostChannelOpenAckResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	// check following:
	// - exists: channel<INIT>
	channel, found := k.ChannelKeeper.GetChannel(ctx, msg.PortId, msg.ChannelId)
	if !found {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannel, "not found")
	}
	if channel.State != channeltypes.INIT {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelState, "expected channel to be in INIT state")
	}
	// TODO: Verify channel TRYOPEN on counterparty.
	k.ChannelKeeper.WriteOpenAckChannel(ctx, msg.PortId, msg.ChannelId, msg.CounterpartyVersion, msg.CounterpartyChannelId)

	ctx.Logger().Info("post channel open ack callback succeeded", "channel-id", msg.ChannelId, "port-id", msg.PortId)

	return &vibctypes.MsgPostChannelOpenAckResponse{}, nil
}

func (k Keeper) PostChannelOpenConfirm(goCtx context.Context, msg *vibctypes.MsgPostChannelOpenConfirm) (*vibctypes.MsgPostChannelOpenConfirmResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	// check following:
	// - exists: channel<TRYOPEN>
	channel, found := k.ChannelKeeper.GetChannel(ctx, msg.PortId, msg.ChannelId)
	if !found {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannel, "not found")
	}
	if channel.State != channeltypes.TRYOPEN {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelState, "expected channel to be in TRYOPEN state")
	}
	// TODO: Verify channel OPEN on counterparty.
	k.ChannelKeeper.WriteOpenConfirmChannel(ctx, msg.PortId, msg.ChannelId)

	ctx.Logger().Info("post channel open confirm callback succeeded", "channel-id", msg.ChannelId, "port-id", msg.PortId)

	return &vibctypes.MsgPostChannelOpenConfirmResponse{}, nil
}

func (k Keeper) SendPacket(goCtx context.Context, msg *vibctypes.MsgSendPacket) (*vibctypes.MsgSendPacketResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	channelCap, ok := k.scopedKeeper.GetCapability(ctx, host.ChannelCapabilityPath(msg.SourcePortId, msg.SourceChannelId))
	if !ok {
		return nil, sdkerrors.Wrap(channeltypes.ErrChannelCapabilityNotFound, "module does not own channel capability")
	}

	sequence, err := k.ChannelKeeper.SendPacket(ctx, channelCap, msg.SourcePortId, msg.SourceChannelId,
		msg.TimeoutHeight, msg.TimeoutTimestamp, msg.Data)
	if err != nil {
		return nil, sdkerrors.Wrap(err, "send packet failed")
	}
	return &vibctypes.MsgSendPacketResponse{
		Sequence: sequence,
	}, nil
}

func (k Keeper) PostRecvPacket(goCtx context.Context, msg *vibctypes.MsgPostRecvPacket) (*vibctypes.MsgPostRecvPacketResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	// check following:
	// - exists: channel<OPEN>
	// - ORDERED: packet seq + 1 == next seq recv
	// - UNORDERED: exists packet receipt
	channel, found := k.ChannelKeeper.GetChannel(ctx, msg.Packet.DestinationPort, msg.Packet.DestinationChannel)
	if !found {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannel, "not found")
	}
	if channel.State != channeltypes.OPEN {
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelState, "expected channel to be in OPEN state")
	}

	switch channel.Ordering {
	case channeltypes.ORDERED:
		_, found := k.ChannelKeeper.GetPacketReceipt(ctx, msg.Packet.DestinationPort, msg.Packet.DestinationChannel,
			msg.Packet.Sequence)
		if !found {
			return nil, sdkerrors.Wrap(channeltypes.ErrInvalidPacket, "packet receipt not found")
		}
	case channeltypes.UNORDERED:
		nextSequenceRecv, found := k.ChannelKeeper.GetNextSequenceRecv(ctx, msg.Packet.DestinationPort, msg.Packet.DestinationChannel)
		if !found {
			return nil, channeltypes.ErrSequenceReceiveNotFound
		}
		if msg.Packet.Sequence+1 != nextSequenceRecv {
			return nil, sdkerrors.Wrap(channeltypes.ErrPacketSequenceOutOfOrder, "expected packet seq + 1 == next seq recv")
		}
	default:
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelOrdering, "expecting ORDERED or UNORDERED")
	}

	// TODO: Verify packet commitment on counterparty.
	channelCap, ok := k.scopedKeeper.GetCapability(ctx, host.ChannelCapabilityPath(msg.Packet.DestinationPort, msg.Packet.DestinationChannel))
	if !ok {
		return nil, sdkerrors.Wrap(channeltypes.ErrChannelCapabilityNotFound, "module does not own channel capability")
	}

	if err := k.ChannelKeeper.WriteAcknowledgement(ctx, channelCap, msg.Packet, msg.Acknowledgement); err != nil {
		return nil, sdkerrors.Wrap(err, "write acknowledgement failed")
	}

	defer func() {
		telemetry.IncrCounterWithLabels(
			[]string{"tx", "msg", "ibc", channeltypes.EventTypeRecvPacket},
			1,
			[]metrics.Label{
				telemetry.NewLabel(coretypes.LabelSourcePort, msg.Packet.SourcePort),
				telemetry.NewLabel(coretypes.LabelSourceChannel, msg.Packet.SourceChannel),
				telemetry.NewLabel(coretypes.LabelDestinationPort, msg.Packet.DestinationPort),
				telemetry.NewLabel(coretypes.LabelDestinationChannel, msg.Packet.DestinationChannel),
			},
		)
	}()

	ctx.Logger().Info("post receive packet callback succeeded", "port-id", msg.Packet.SourcePort, "channel-id", msg.Packet.SourceChannel, "result", channeltypes.SUCCESS.String())

	return &vibctypes.MsgPostRecvPacketResponse{}, nil
}

func (k Keeper) PostAcknowledgementPacket(goCtx context.Context, msg *vibctypes.MsgPostAcknowledgementPacket) (*vibctypes.MsgPostAcknowledgementPacketResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	channelCap, ok := k.scopedKeeper.GetCapability(ctx, host.ChannelCapabilityPath(msg.Packet.DestinationPort, msg.Packet.DestinationChannel))
	if !ok {
		return nil, sdkerrors.Wrap(channeltypes.ErrChannelCapabilityNotFound, "module does not own channel capability")
	}

	// TODO: Verify packet acknowledgement on counterparty

	// Delete packet commitment, since the packet has been acknowledged, the commitement is no longer necessary
	if err := k.ChannelKeeper.PostAcknowledgePacket(ctx, channelCap, msg.Packet); err != nil {
		return nil, sdkerrors.Wrap(err, "post acknowledgement error")
	}

	defer func() {
		telemetry.IncrCounterWithLabels(
			[]string{"tx", "msg", "ibc", channeltypes.EventTypeAcknowledgePacket},
			1,
			[]metrics.Label{
				telemetry.NewLabel(coretypes.LabelSourcePort, msg.Packet.SourcePort),
				telemetry.NewLabel(coretypes.LabelSourceChannel, msg.Packet.SourceChannel),
				telemetry.NewLabel(coretypes.LabelDestinationPort, msg.Packet.DestinationPort),
				telemetry.NewLabel(coretypes.LabelDestinationChannel, msg.Packet.DestinationChannel),
			},
		)
	}()

	ctx.Logger().Info("post acknowledgement succeeded", "port-id", msg.Packet.SourcePort, "channel-id", msg.Packet.SourceChannel, "result", channeltypes.SUCCESS.String())

	return nil, nil
}

func (k Keeper) PostTimeoutPacket(goCtx context.Context, msg *vibctypes.MsgPostTimeoutPacket) (*vibctypes.MsgPostTimeoutPacketResponse, error) {
	ctx := sdk.UnwrapSDKContext(goCtx)

	// Lookup module by channel capability
	_, cap, err := k.ChannelKeeper.LookupModuleByChannel(ctx, msg.Packet.SourcePort, msg.Packet.SourceChannel)
	if err != nil {
		ctx.Logger().Error("timeout failed", "port-id", msg.Packet.SourcePort, "channel-id", msg.Packet.SourceChannel, "error", sdkerrors.Wrap(err, "could not retrieve module from port-id"))
		return nil, sdkerrors.Wrap(err, "could not retrieve module from port-id")
	}

	// TODO: Verify recv seq has not been incremented (ordered) or packet receipt absence (unordered).

	// Delete packet commitment
	if err = k.ChannelKeeper.TimeoutExecuted(ctx, cap, msg.Packet); err != nil {
		return nil, err
	}

	defer func() {
		telemetry.IncrCounterWithLabels(
			[]string{"ibc", "timeout", "packet"},
			1,
			[]metrics.Label{
				telemetry.NewLabel(coretypes.LabelSourcePort, msg.Packet.SourcePort),
				telemetry.NewLabel(coretypes.LabelSourceChannel, msg.Packet.SourceChannel),
				telemetry.NewLabel(coretypes.LabelDestinationPort, msg.Packet.DestinationPort),
				telemetry.NewLabel(coretypes.LabelDestinationChannel, msg.Packet.DestinationChannel),
				telemetry.NewLabel(coretypes.LabelTimeoutType, "height"),
			},
		)
	}()

	ctx.Logger().Info("post timeout packet callback succeeded", "port-id", msg.Packet.SourcePort, "channel-id", msg.Packet.SourceChannel, "result", channeltypes.SUCCESS.String())
	return nil, nil
}
