package kv

// PrefixEnd kiszámítja az adott prefixhez tartózó tartomány végét
// "foo"-ból "fop" lesz, hiszen o + 1 = p, és így a [foo, fop) csak a foo-t figja visszaadni
// 
// Ha a prefix üres, vagy csak 0xFF byte-okból áll, akkor nincs olyan kulcs, ami a prefix-szel kezdődik, így nil-t adunk vissza
func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1] // drop trailing 0xFF bytes
		}
	}
	return nil
}
