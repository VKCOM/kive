package ktypes

import (
	"testing"
	"time"
)

func TestHoursRange(t *testing.T) {
	r := HoursRange(1637951101000, 1537951101000)
	if len(r) != 0 {
		t.Errorf("wrog HoursRange: %+v", HoursRange(1537951101000, 1537951101000))
	}

	r = HoursRange(1537951101000, 1537951101000)
	if r[0] != "000000000427208" || len(r) != 1 {
		t.Errorf("wrog HoursRange: %+v", HoursRange(1537951101000, 1537951101000))
	}

	r = HoursRange(1537951101000, 1537954701000)
	if r[0] != "000000000427208" || r[1] != "000000000427209" || len(r) != 2 {
		t.Errorf("wrog HoursRange: %+v", HoursRange(1537951101000, 1537954701000))
	}
}

func TestChunkKey_Duration(t *testing.T) {
	ci := ChunkKey{Ts: 5000}
	if ci.Duration() != 5*time.Second {
		t.Errorf("bad duration")
	}
}
