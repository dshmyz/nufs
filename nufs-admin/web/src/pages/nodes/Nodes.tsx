import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { getNodes, decommissionNode, NodeInfo } from '../../api/client'

export default function Nodes() {
  const { clusterId } = useParams()
  const [nodes, setNodes] = useState<NodeInfo[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!clusterId) return
    getNodes(clusterId)
      .then(setNodes)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [clusterId])

  const handleDecommission = async (nodeId: string) => {
    if (!clusterId) return
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
              <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{node.id}</td>
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
    </div>
  )
}