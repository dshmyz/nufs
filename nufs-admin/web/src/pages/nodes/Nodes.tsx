import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import {
  getNodes,
  decommissionNode,
  getNodeDisks,
  nodeDiskAction,
  nodeGcScan,
  NodeInfo,
  NodeDisk,
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
  const [adoptDir, setAdoptDir] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState<Record<string, boolean>>({})

  const loadNodes = () => {
    if (!clusterId) return
    setLoading(true)
    getNodes(clusterId)
      .then(setNodes)
      .catch(console.error)
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    loadNodes()
  }, [clusterId])

  const toggleDisks = async (nodeId: string) => {
    if (!clusterId) return
    const next = { ...expanded, [nodeId]: !expanded[nodeId] }
    setExpanded(next)
    if (next[nodeId] && !disks[nodeId]) {
      try {
        const list = await getNodeDisks(clusterId, nodeId)
        setDisks(d => ({ ...d, [nodeId]: list }))
      } catch (err) {
        console.error(err)
      }
    }
  }

  const refreshDisks = async (nodeId: string) => {
    if (!clusterId) return
    try {
      const list = await getNodeDisks(clusterId, nodeId)
      setDisks(d => ({ ...d, [nodeId]: list }))
    } catch (err) {
      console.error(err)
    }
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
    } catch (err) {
      console.error(err)
    }
  }

  if (loading) return <div>加载中...</div>

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>节点列表</h1>

      <table style={{
        width: '100%',
        background: '#fff',
        border: '1px solid #e2e6ec',
        borderRadius: '10px',
        borderCollapse: 'collapse',
      }}>
        <thead>
          <tr style={{ background: '#f5f6f8' }}>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>ID</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>地址</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>状态</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>容量</th>
            <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map(node => (
            <tr key={node.id}>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                {node.id}
                <button
                  onClick={() => toggleDisks(node.id)}
                  style={{
                    marginLeft: '8px',
                    padding: '2px 8px',
                    fontSize: '12px',
                    background: '#eff6ff',
                    color: '#2563eb',
                    border: '1px solid #bfdbfe',
                    borderRadius: '4px',
                    cursor: 'pointer',
                  }}
                >
                  {expanded[node.id] ? '收起磁盘' : '磁盘'}
                </button>
              </td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{node.address}</td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                <span style={{
                  padding: '2px 8px',
                  borderRadius: '4px',
                  background: node.status === 'online' ? '#d1fae5' : '#fee2e2',
                  color: node.status === 'online' ? '#047857' : '#b91c1c',
                }}>
                  {node.status}
                </span>
              </td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                {node.used} / {node.capacity} GB
              </td>
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>
                <button
                  onClick={() => handleDecommission(node.id)}
                  style={{
                    padding: '4px 12px',
                    background: '#fee2e2',
                    color: '#dc2626',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                  }}
                >
                  下线
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {Object.keys(expanded).filter(k => expanded[k]).map(nodeId => (
        <div key={nodeId} style={{
          background: '#fff',
          border: '1px solid #e2e6ec',
          borderRadius: '10px',
          padding: '16px',
          marginTop: '12px',
        }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '12px' }}>
                <h3 style={{ fontSize: '15px', fontWeight: 700, margin: 0 }}>节点 {nodeId} · 磁盘运维</h3>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                  <button
                    onClick={() => handleGc(nodeId)}
                    disabled={busy[`${nodeId}:gc`]}
                    style={{ padding: '4px 10px', background: '#fef3c7', color: '#92400e', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                  >
                    {busy[`${nodeId}:gc`] ? '触发中...' : 'GC 扫描'}
                  </button>
                  <button
                    onClick={() => handleDrain(nodeId)}
                    disabled={busy[`${nodeId}:drain`]}
                    style={{ padding: '4px 10px', background: '#fef3c7', color: '#92400e', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                  >
                    排空写入
                  </button>
                </div>
              </div>

              {!disks[nodeId] || disks[nodeId].length === 0 ? (
                <div style={{ color: '#5a6478', fontSize: '13px', padding: '12px' }}>无磁盘</div>
              ) : (
                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                  <thead>
                    <tr style={{ background: '#f5f6f8' }}>
                      <th style={{ padding: '8px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>槽位</th>
                      <th style={{ padding: '8px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>目录</th>
                      <th style={{ padding: '8px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>状态</th>
                      <th style={{ padding: '8px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>chunk</th>
                      <th style={{ padding: '8px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}></th>
                    </tr>
                  </thead>
                  <tbody>
                    {(disks[nodeId] || []).map((disk, i) => (
                      <tr key={i}>
                        <td style={{ padding: '8px', borderBottom: '1px solid #e2e6ec' }}>bay {disk.index ?? '?'}</td>
                        <td style={{ padding: '8px', borderBottom: '1px solid #e2e6ec', fontFamily: 'monospace', fontSize: '12px' }}>{disk.dir}</td>
                        <td style={{ padding: '8px', borderBottom: '1px solid #e2e6ec' }}>{disk.state}</td>
                        <td style={{ padding: '8px', borderBottom: '1px solid #e2e6ec' }}>{disk.chunks ?? 0}</td>
                        <td style={{ padding: '8px', borderBottom: '1px solid #e2e6ec' }}>
                          <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                            {DISK_ACTIONS.map(a => (
                              <button
                                key={a.action}
                                onClick={() => handleDiskAction(nodeId, a.action, disk.dir!)}
                                disabled={busy[`${nodeId}:${a.action}:${disk.dir}`]}
                                style={{
                                  padding: '3px 10px',
                                  fontSize: '12px',
                                  background: a.action === 'decommission' || a.action === 'retire' ? '#fee2e2' : '#eff6ff',
                                  color: a.action === 'decommission' || a.action === 'retire' ? '#dc2626' : '#2563eb',
                                  border: '1px solid ' + (a.action === 'decommission' || a.action === 'retire' ? '#fecaca' : '#bfdbfe'),
                                  borderRadius: '4px',
                                  cursor: 'pointer',
                                }}
                              >
                                {a.label}
                              </button>
                            ))}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}

              <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginTop: '12px', paddingTop: '12px', borderTop: '1px solid #e2e6ec' }}>
                <input
                  value={adoptDir[nodeId] || ''}
                  onChange={e => setAdoptDir(d => ({ ...d, [nodeId]: e.target.value }))}
                  placeholder="/mnt/disk3   (挂载为新磁盘的路径)"
                  style={{ flex: 1, padding: '6px 8px', border: '1px solid #e2e6ec', borderRadius: '6px', fontSize: '12px' }}
                />
                <button
                  onClick={() => handleAdopt(nodeId)}
                  disabled={busy[`${nodeId}:adopt`]}
                  style={{ padding: '6px 14px', background: '#1f6feb', color: '#fff', border: 'none', borderRadius: '6px', cursor: 'pointer' }}
                >
                  {busy[`${nodeId}:adopt`] ? '采纳中...' : '采纳新磁盘'}
                </button>
              </div>
            </div>
          ))}
    </div>
  )
}
