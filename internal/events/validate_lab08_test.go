package events

import "testing"

func TestValidateLab08(t *testing.T) {
	valid := Event{ID: "evt-001", Kind: "auth", Severity: 2, Message: "login denied"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("корректное событие: %v", err)
	}
	valid.Severity = 11
	if err := valid.Validate(); err == nil {
		t.Fatal("ошибочная серьёзность принята")
	}
}
