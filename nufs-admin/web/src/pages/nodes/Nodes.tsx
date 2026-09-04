import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import {
  getNodes,
  decommissionNode,
  getNodeDisks,
  nodeDiskAction,
  nodeGcScan,
  getNodeMetrics,
  getNodeAlerts,
  NodeInfo,
  NodeDisk,
  NodeMetrics,
  NodeAlert,
} from '../../api/client'

const DISK_ACTIONS = [
  { action: 'verify', label: '校验', confirm: false },
  { action: 'migrate', label: '迁移', confirm: true, msg: '迁移该磁盘上的 chunk 到其他磁盘？' },
  { action: 'decommission', label: '下线', confirm: true, msg: '下线会将该磁盘的 chunk 迁移到其他磁盘后移除。是否继续？' },
  { action: 'retire', label: '报废', confirm: true, msg: '报废立即移除磁盘且不迁移 chunk（仅故障盘，存在数据丢失风险）。是否继续？' },
]

export default function Nodes() {
  const { clusterId } = useParams()
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [disks, setDisks] = useState<Record<string, NodeDisk[]>>({})
  const [metrics, setMetrics] = useState<Record<string, NodeMetrics>>({})
  const [alerts, setAlerts] = useState<Record<string, NodeAlert[]>>({})
  const [rateHist, setRateHist] = useState<Record<string, { r: number[]; w: number[] }>>({})
  const [adoptDir, setAdoptDir] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState<Record<string, boolean>>({})

  const loadNodes = () => {
    if (!clusterId) return
    setLoading(true)
    getNodes(clusterId).then(setNodes).catch(console.error).finally(() => setLoading(false))
  }

  useEffect(() => { loadNodes() }, [clusterId])

  useEffect(() => {
    if (!clusterId) return
    const expandedIds = Object.keys(expanded).filter(k => expanded[k])
    if (expandedIds.length === 0) return
    const poll = async () => {
      for (const id of expandedIds) {
        try {
          const m = await getNodeMetrics(clusterId, id)
          setMetrics(prev => ({ ...prev, [id]: m }))
          setRateHist(prev => {
            const cur = prev[id] || { r: [], w: [] }
            const r = [...cur.r, m?.disk?.read_iops ?? 0].slice(-30)
            const w = [...cur.w, m?.disk?.write_iops ?? 0].slice(-30)
            return { ...prev, [id]: { r, w } }
          })
        } catch { /* ignore */ }
      }
    }
    poll()
    const timer = setInterval(poll, 2000)
    return () => clearInterval(timer)
  }, [clusterId, expanded])

  useEffect(() => {
    if (!clusterId) return
    Object.keys(expanded).filter(k => expanded[k]).forEach(async (id) => {
      if (alerts[id]) return
      try {
        const a = await getNodeAlerts(clusterId, id)
        setAlerts(prev => ({ ...prev, [id]: a }))
      } catch { /* ignore */ }
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusterId, expanded])

  const toggleDisks = async (nodeId: string) => {
    if (!clusterId) return
    const next = { ...expanded, [nodeId]: !expanded[nodeId] }
    setExpanded(next)
    if (next[nodeId] && !disks[nodeId]) {
      try {
        const list = await getNodeDisks(clusterId, nodeId)
        setDisks(d => ({ ...d, [nodeId]: list }))
      } catch (err) { console.error(err) }
    }
  }

  const refreshDisks = async (nodeId: string) => {
    if (!clusterId) return
    try {
      const list = await getNodeDisks(clusterId, nodeId)
      setDisks(d => ({ ...d, [nodeId]: list }))
    } catch (err) { console.error(err) }
  }

  const handleDiskAction = async (nodeId: string, action: string, dir: string) => {
    if (!clusterId) return
    const meta = DISK_ACTIONS.find(a => a.action === action)
    if (meta?.confirm && !window.confirm(meta.msg)) return
    const key = `${nodeId}:${action}:${dir}`
    setBusy(b => ({ ...b, [key]: true }))
    try {
      await nodeDiskAction(clusterId, nodeId, action, dir)
      await refreshDisks(nodeId)
    } catch (err) {
      console.error(err)
      window.alert('操作失败：' + (err as any)?.response?.data?.error || String(err))
    } finally {
      setBusy(b => ({ ...b, [key]: false }))
    }
  }

  const handleAdopt = async (nodeId: string) => {
    if (!clusterId) return
    const dir = (adoptDir[nodeId] || '').trim()
    if (!dir) return window.alert('请输入要采纳的磁盘目录')
    setBusy(b => ({ ...b, [`${nodeId}:adopt`]: true }))
    try {
      await nodeDiskAction(clusterId, nodeId, 'adopt', dir)
      await refreshDisks(nodeId)
    } catch (err) {
      console.error(err)
      window.alert('采纳失败：' + (err as any)?.response?.data?.error || String(err))
    } finally {
      setBusy(b => ({ ...b, [`${nodeId}:adopt`]: false }))
    }
  }

  const handleDrain = async (nodeId: string) => {
    if (!clusterId) return
    if (!window.confirm('排空该节点写入（重启前）。是否继续？')) return
    setBusy(b => ({ ...b, [`${nodeId}:drain`]: true }))
    try {
      await nodeDiskAction(clusterId, nodeId, 'drain')
    } catch (err) {
      console.error(err)
      window.alert('排空失败：' + (err as any)?.response?.data?.error || String(err))
    } finally {
      setBusy(b => ({ ...b, [`${nodeId}:drain`]: false }))
    }
  }

  const handleGc = async (nodeId: string) => {
    if (!clusterId) return
    if (!window.confirm('触发该节点的孤儿 chunk GC 扫描。是否继续？')) return
    setBusy(b => ({ ...b, [`${nodeId}:gc`]: true }))
    try {
      await nodeGcScan(clusterId, nodeId)
      window.alert('GC 扫描已触发')
    } catch (err) {
      console.error(err)
      window.alert('GC 触发失败：' + (err as any)?.response?.data?.error || String(err))
    } finally {
      setBusy(b => ({ ...b, [`${nodeId}:gc`]: false }))
    }
  }

  const handleDecommission = async (nodeId: string) => {
    if (!clusterId) return
    if (!window.confirm(`下线节点 ${nodeId} 为一次性不可逆操作。是否继续？`)) return
    try {
      await decommissionNode(clusterId, nodeId)
      setNodes(nodes.filter(n => n.id !== nodeId))
    } catch (err) { console.error(err) }
  }

  if (loading) return <div style={{ padding: '24px', color: 'var(--text-dim)' }}>加载中...</div>

  return (
    <div>
      <div className="page-head">
        <div>
          <div className="eyebrow">Nodes</div>
          <h1>节点列表</h1>
        </div>
      </div>

      <div className="panel" style={{ overflowX: 'auto' }}>
        <table className="dt">
          <thead>
            <tr>
              <th>ID</th><th>地址</th><th>状态</th><th>容量</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map(node => (
              <tr key={node.id}>
                <td className="strong">
                  {node.id}
                  <button className="btn btn-sm" style={{ marginLeft: 8 }} onClick={() => toggleDisks(node.id)}>
                    {expanded[node.id] ? '收起磁盘' : '磁盘'}
                  </button>
                </td>
                <td>{node.address}</td>
                <td>
                  <span className={`badge ${node.status === 'online' ? 'badge-ok' : 'badge-danger'}`}>
                    <span className={`led led-${node.status === 'online' ? 'ok' : 'danger'}`} />
                    {node.status}
                  </span>
                </td>
                <td className="mono">{node.used} / {node.capacity} GB</td>
                <td>
                  <button className="btn btn-sm btn-danger" onClick={() => handleDecommission(node.id)}>下线</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {Object.keys(expanded).filter(k => expanded[k]).map(nodeId => (
        <div key={nodeId} className="panel panel-pad" style={{ marginTop: 14 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <h3>节点 {nodeId} · 磁盘运维</h3>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-sm btn-warn" onClick={() => handleGc(nodeId)} disabled={busy[`${nodeId}:gc`]}>
                {busy[`${nodeId}:gc`] ? '触发中...' : 'GC 扫描'}
              </button>
              <button className="btn btn-sm btn-warn" onClick={() => handleDrain(nodeId)} disabled={busy[`${nodeId}:drain`]}>
                排空写入
              </button>
            </div>
          </div>

          {/* 指标条 */}
          {metrics[nodeId]?.disk && (
            <div style={{ display: 'flex', gap: 22, flexWrap: 'wrap', marginBottom: 12, padding: '10px 12px', background: 'var(--bg-hover)', borderRadius: 8, fontSize: 13 }}>
              <div>
                <div className="faint" style={{ fontSize: 12 }}>容量使用率</div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <div style={{ width: 110, height: 8, background: 'var(--line)', borderRadius: 4, overflow: 'hidden' }}>
                    <div style={{ width: `${Math.min(100, metrics[nodeId].disk!.usage_pct ?? 0)}%`, height: '100%', background: (metrics[nodeId].disk!.usage_pct ?? 0) > 85 ? 'var(--danger)' : 'var(--accent)' }} />
                  </div>
                  <b className="mono">{(metrics[nodeId].disk!.usage_pct ?? 0).toFixed(1)}%</b>
                </div>
              </div>
              <div>
                <div className="faint" style={{ fontSize: 12 }}>读</div>
                <b className="mono">{metrics[nodeId].disk!.read_iops ?? 0} IOPS</b>
                <Sparkline data={rateHist[nodeId]?.r} />
              </div>
              <div>
                <div className="faint" style={{ fontSize: 12 }}>写</div>
                <b className="mono">{metrics[nodeId].disk!.write_iops ?? 0} IOPS</b>
                <Sparkline data={rateHist[nodeId]?.w} />
              </div>
              <div><div className="faint" style={{ fontSize: 12 }}>IO 错误</div><b className="mono" style={{ color: (metrics[nodeId].disk!.io_errors ?? 0) > 0 ? 'var(--danger)' : 'var(--text)' }}>{metrics[nodeId].disk!.io_errors ?? 0}</b></div>
              <div><div className="faint" style={{ fontSize: 12 }}>chunk</div><b className="mono">{metrics[nodeId].disk!.chunk_count ?? 0}</b></div>
            </div>
          )}

          {!disks[nodeId] || disks[nodeId].length === 0 ? (
            <div className="muted" style={{ fontSize: 13, padding: 12 }}>无磁盘</div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table className="dt">
                <thead>
                  <tr><th>槽位</th><th>目录</th><th>状态</th><th>chunk</th><th>使用</th><th></th></tr>
                </thead>
                <tbody>
                  {(disks[nodeId] || []).map((disk, i) => (
                    <tr key={i}>
                      <td className="mono">bay {disk.index ?? '?'}</td>
                      <td className="mono" style={{ fontSize: 12 }}>{disk.dir}</td>
                      <td><span className={`badge ${disk.failed ? 'badge-danger' : 'badge-ok'}`}>{disk.state}</span></td>
                      <td className="mono">{disk.chunks ?? 0}</td>
                      <td className="mono" style={{ fontSize: 12 }}>
                        {disk.total_bytes ? (
                          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                            <div style={{ width: 80, height: 6, background: 'var(--line)', borderRadius: 3, overflow: 'hidden' }}>
                              <div style={{ width: `${Math.min(100, (disk.bytes! / disk.total_bytes) * 100)}%`, height: '100%', background: 'var(--accent)' }} />
                            </div>
                            <span className="faint">{(disk.bytes! / disk.total_bytes * 100).toFixed(1)}%</span>
                          </div>
                        ) : <span className="faint">—</span>}
                      </td>
                      <td>
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                          {DISK_ACTIONS.map(a => {
                            const danger = a.action === 'decommission' || a.action === 'retire'
                            return (
                              <button
                                key={a.action}
                                className={`btn btn-sm ${danger ? 'btn-danger' : ''}`}
                                onClick={() => handleDiskAction(nodeId, a.action, disk.dir!)}
                                disabled={busy[`${nodeId}:${a.action}:${disk.dir}`]}
                              >
                                {a.label}
                              </button>
                            )
                          })}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 12, paddingTop: 12, borderTop: '1px solid var(--line)' }}>
            <input
              value={adoptDir[nodeId] || ''}
              onChange={e => setAdoptDir(d => ({ ...d, [nodeId]: e.target.value }))}
              placeholder="/mnt/disk3   挂载为新磁盘的路径"
              style={{ flex: 1 }}
            />
            <button className="btn btn-primary" onClick={() => handleAdopt(nodeId)} disabled={busy[`${nodeId}:adopt`]}>
              {busy[`${nodeId}:adopt`] ? '采纳中...' : '采纳新磁盘'}
            </button>
          </div>

          {alerts[nodeId] && alerts[nodeId].length > 0 && (
            <div style={{ marginTop: 12, paddingTop: 10, borderTop: '1px solid var(--line)' }}>
              <div className="faint" style={{ fontSize: 12, fontWeight: 700, marginBottom: 6 }}>最近告警</div>
              {alerts[nodeId].slice(-6).reverse().map((a, i) => (
                <div key={i} style={{ display: 'flex', gap: 10, alignItems: 'center', fontSize: 12, padding: '3px 0' }}>
                  <span className={`badge ${a.level === 'critical' ? 'badge-danger' : a.level === 'warning' ? 'badge-warn' : 'badge-accent'}`}>{a.level}</span>
                  <span className="mono">使用率 {(a.usage_pct ?? 0).toFixed(1)}%</span>
                  {a.ts && <span className="faint">{new Date(a.ts).toLocaleString()}</span>}
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

// 迷你读写速率趋势线（最近 30 个采样，2s 一个点 → 约 1 分钟）
function Sparkline({ data }: { data?: number[] }) {
  if (!data || data.length < 2) return <div style={{ height: 18 }} />
  const W = 64, H = 18
  const max = Math.max(...data, 1)
  const pts = data.map((v, i) => `${(i / (data.length - 1)) * W},${H - (v / max) * (H - 2) - 1}`).join(' ')
  return (
    <svg width={W} height={H} style={{ display: 'block', marginTop: 2 }}>
      <polyline points={pts} fill="none" stroke="var(--accent)" strokeWidth="1.5" />
    </svg>
  )
}
