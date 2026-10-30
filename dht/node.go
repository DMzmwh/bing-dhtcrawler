package dht

import (
	"fmt"
	"net/netip"
	"time"
)

type Node struct {
	ID              NodeID
	Addr            netip.AddrPort
	lastRespondedAt time.Time
	bep51           int //0未知，1支持，2不支持
	nextSampleTime  time.Time
}

func (n Node) DebugString() string {
	return fmt.Sprintf(
		"ID=%x Addr=%s BEP51=%d Last=%s NextSample=%s",
		n.ID,
		n.Addr,
		n.bep51,
		n.lastRespondedAt.Format("15:04:05"),
		n.nextSampleTime.Format("15:04:05"),
	)
}
func (n *Node) LastAtTime() time.Time {
	return n.lastRespondedAt
}

func (n *Node) LastAt() {
	n.lastRespondedAt = time.Now()
}

func (n *Node) isBep51() bool {
	if n.bep51 != 2 {
		return true
	}
	return false
}

func (n *Node) IsSampleInfoHashesCheck() bool {
	if !n.isBep51() {
		return false
	}
	threshold := time.Now().Add(-(5 * time.Second))
	return n.nextSampleTime.Before(threshold)
}

func (n *Node) Bep51(bep51 int) {
	n.bep51 = bep51
	n.lastRespondedAt = time.Now()
}
func (n *Node) NextSampleTime(in int64) {
	n.nextSampleTime = time.Now().Add(time.Duration(in) * time.Second)
}

func (n *Node) NextSampleTimeTime() time.Time {
	return n.nextSampleTime
}
