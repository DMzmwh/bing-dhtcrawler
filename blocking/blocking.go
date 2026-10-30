package blocking

import "net/netip"

type Manager interface {
	IsBlockedInfoHash(infoHash [20]byte) bool
	IsBlockedPeer(addr netip.AddrPort) bool
}
