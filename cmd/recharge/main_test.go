package main

import "testing"

// loopbackAddr is the gate on whether an authz-disabled server is allowed to
// start: loopback is a pilot, anything else is an incident. It decides money-
// exposing behavior, so it is pinned here. A wrong "true" opens :8080 to the
// world with authz off.
func TestLoopbackAddr(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		// Loopback -- safe for the insecure pilot.
		{"127.0.0.1:8080", true},
		{"localhost:8080", true},
		{"[::1]:8080", true},
		{"127.0.0.1:0", true},

		// NOT loopback -- authz-off here is world-readable and must be refused.
		{":8080", false},        // empty host binds every interface
		{"0.0.0.0:8080", false}, // all interfaces, explicitly
		{"192.168.1.10:8080", false},
		{"10.0.0.5:8080", false},
		{"[::]:8080", false}, // unspecified v6
		{"example.com:8080", false},
	}

	for _, c := range cases {
		if got := loopbackAddr(c.addr); got != c.want {
			t.Errorf("loopbackAddr(%q) = %v, want %v", c.addr, got, c.want)
		}
	}
}
