package utils

import "testing"

func TestParseSekaiServerRegion(t *testing.T) {
	valid := []HarukiSekaiServerRegion{
		HarukiSekaiServerRegionJP,
		HarukiSekaiServerRegionEN,
		HarukiSekaiServerRegionTW,
		HarukiSekaiServerRegionKR,
		HarukiSekaiServerRegionCN,
	}
	for _, region := range valid {
		got, err := ParseSekaiServerRegion(string(region))
		if err != nil {
			t.Fatalf("expected region %s to be valid: %v", region, err)
		}
		if got != region {
			t.Fatalf("unexpected parsed region: got %s, want %s", got, region)
		}
	}

	if _, err := ParseSekaiServerRegion("xx"); err == nil {
		t.Fatalf("expected invalid region to return error")
	}
}
