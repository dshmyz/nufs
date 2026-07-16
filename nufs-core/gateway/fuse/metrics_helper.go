package fuse

// recorderFor 安全获取 recorder，nil 返回 noopMetricsRecorder。
// 供 DFSFile/DFSDir/DFSSymlink 在测试场景（recorder 未注入）下安全调用。
func recorderFor(r MetricsRecorder) MetricsRecorder {
	if r == nil {
		return noopMetricsRecorder{}
	}
	return r
}
