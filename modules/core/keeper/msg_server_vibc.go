package keeper

import (
	"context"

	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"

	channeltypes "github.com/cosmos/ibc-go/v7/modules/core/04-channel/types"
	host "github.com/cosmos/ibc-go/v7/modules/core/24-host"
	vibctypes "github.com/cosmos/ibc-go/v7/modules/core/vibc/types"
)

var _ vibctypes.VirtualIBCSidecarServer = Keeper{}

func (k Keeper) WriteOpenInitOrTryChan(goCtx context.Context, msg *vibctypes.MsgWriteOpenInitOrTryChan) (*vibctypes.MsgWriteOpenInitOrTryChanResponse, error) {
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
		k.ChannelKeeper.WriteOpenTryChannel(ctx, msg.PortId, msg.ChannelId, msg.Channel.Ordering, msg.Channel.ConnectionHops,
			msg.Channel.Counterparty, msg.Channel.Version)
	default:
		return nil, sdkerrors.Wrap(channeltypes.ErrInvalidChannelState, "expecting INIT or TRYOPEN")
	}
	return &vibctypes.MsgWriteOpenInitOrTryChanResponse{}, nil
}

func (k Keeper) WriteOpenAckChan(goCtx context.Context, msg *vibctypes.MsgWriteOpenAckChan) (*vibctypes.MsgWriteOpenAckChanResponse, error) {
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
	return &vibctypes.MsgWriteOpenAckChanResponse{}, nil
}

func (k Keeper) WriteOpenConfirmChan(goCtx context.Context, msg *vibctypes.MsgWriteOpenConfirmChan) (*vibctypes.MsgWriteOpenConfirmChanResponse, error) {
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
	return nil, nil
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

func (k Keeper) WritePacketAck(goCtx context.Context, msg *vibctypes.MsgWritePacketAck) (*vibctypes.MsgWritePacketAckResponse, error) {
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

	return &vibctypes.MsgWritePacketAckResponse{}, nil
}

func (k Keeper) EmitAckPacketEvent(goCtx context.Context, msg *vibctypes.MsgEmitAckPacketEvent) (*vibctypes.MsgEmitAckPacketEventResponse, error) {
	return nil, nil
}

func (k Keeper) EmitTimeoutPacketEvent(goCtx context.Context, msg *vibctypes.MsgEmitTimeoutPacketEvent) (*vibctypes.MsgEmitTimeoutPacketEventResponse, error) {
	return nil, nil
}
