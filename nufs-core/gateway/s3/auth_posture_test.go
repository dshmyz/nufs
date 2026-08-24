package s3

import (
	"strings"
	"testing"
)

// TestStartupAuthPosture pins the boot-time auth posture classification that
// nufs-s3's startup logs are derived from. The critical case: a successful
// metad registry sync that returned zero credentials (fresh registry, or a
// rotated --credential-secret-key on metad that makes every stored secret
// undecryptable). Auth is pinned on in that case — every request is rejected
// with 403 — so the posture must NOT be reported as "anonymous mode": that
// would tell an operator the gateway is open when it is actually locked shut.
func TestStartupAuthPosture(t *testing.T) {
	cases := []struct {
		name          string
		authoritative bool
		count         int
		want          AuthPosture
	}{
		{"synced with credentials", true, 3, AuthPostureSynced},
		{"synced but empty — pinned reject-all", true, 0, AuthPostureSyncedEmpty},
		{"local credentials fallback", false, 2, AuthPostureLocal},
		{"no source at all — anonymous", false, 0, AuthPostureAnonymous},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := StartupAuthPosture(tc.authoritative, tc.count); got != tc.want {
				t.Fatalf("StartupAuthPosture(%v, %d) = %v (%q), want %v (%q)",
					tc.authoritative, tc.count, got, got, tc.want, tc.want)
			}
		})
	}
}

// TestStartupAuthPosture_Descriptions ensures the log line rendered from each
// posture is unambiguous — a SyncedEmpty posture must never render as
// "anonymous".
func TestStartupAuthPosture_Descriptions(t *testing.T) {
	if s := AuthPostureSyncedEmpty.String(); s == "" || containsAnonymous(s) {
		t.Fatalf("SyncedEmpty posture must not read as anonymous, got %q", s)
	}
	if s := AuthPostureAnonymous.String(); !containsAnonymous(s) {
		t.Fatalf("Anonymous posture should read as anonymous, got %q", s)
	}
	if AuthPostureSynced.String() == "" {
		t.Fatal("Synced posture renders empty")
	}
}

func containsAnonymous(s string) bool {
	for _, kw := range []string{"anonymous", "no auth", "no-auth"} {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}
