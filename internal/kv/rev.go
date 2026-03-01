package kv

type Revision struct {
	Main int64
	Sub  int64
}

func (this Revision) GreaterThan(that Revision) bool {
	if this.Main > that.Main {
		return true
	}
	if this.Main < that.Main {
		return false
	}
	return this.Sub > that.Sub
}
