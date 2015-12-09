package kapacitor

// User defined function
type UDFNode struct {
	node
	u *pipeline.UDFNode
}

// Create a new UDFNode that sends incoming data to child process
func newUDFNode(et *ExecutingTask, n *pipeline.UDFNode) (*UDFNode, error) {
	un := &UDFNode{
		node: node{Node: n, et: et},
		u:    n,
	}

	en.node.runF = en.runUDF
	return en, nil
}

func (u *UDFNode) runUDF() error {

	go func() {
		switch u.Provides() {
		case pipeline.StreamEdge:
			for p := range u.f.PointOut {
				for _, out := range u.outs {
					out.CollectPoint(p)
				}
			}
		case pipeline.BatchEdge:
			for b := range u.f.BatchOut {
				for _, out := range u.outs {
					out.CollectBatch(b)
				}
			}
		}

	}()

	switch u.Wants() {
	case pipeline.StreamEdge:
		for p, ok := e.u.NextPoint(); ok; p, ok = e.ins[0].NextPoint() {
			u.f.PointIn <- p
		}
		close(u.f.PointIn)
	case pipeline.BatchEdge:
		for b, ok := e.ins[0].NextBatch(); ok; b, ok = e.ins[0].NextBatch() {
			e.f.BatchIn <- b
		}
		close(u.f.BatchIn)
	}
	return nil
}
