import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { getRaftStatus, getAuditLogs, RaftStatus, AuditLog } from '../../api/client'

export default function Governance() {
  const { clusterId } = useParams()
  const [raft, setRaft] = useState<RaftStatus | null>(null)
  const [logs, setLogs] = useState<AuditLog[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!clusterId) return
    Promise.all([
      getRaftStatus(clusterId),
      getAuditLogs(clusterId, { limit: 20 })
    ])
      .then(([raftData, logsData]) => {
        setRaft(raftData)
        setLogs(logsData)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [clusterId])

  if (loading) return <div>加载中...</div>

  return (
    <div>
      <h1 style={{ fontSize: '24px', fontWeight: 700, marginBottom: '24px' }}>集群治理</h1>

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px', marginBottom: '24px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>Raft 状态</h3>
        {raft && (
          <div style={{ fontSize: '13px', color: '#5a6478', lineHeight: 1.8 }}>
            <div>Leader: {raft.leader}</div>
            <div>Term: {raft.term}</div>
            <div>Commit: {raft.commit}</div>
            <div>Applied: {raft.applied}</div>
          </div>
        )}
      </div>

      <div style={{ background: '#fff', border: '1px solid #e2e6ec', borderRadius: '10px', padding: '20px' }}>
        <h3 style={{ fontSize: '16px', fontWeight: 700, marginBottom: '16px' }}>审计日志</h3>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ background: '#f5f6f8' }}>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>时间</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>用户</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>操作</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>资源</th>
              <th style={{ padding: '12px', textAlign: 'left', borderBottom: '1px solid #e2e6ec' }}>结果</th>
            </tr>
          </thead>
          <tbody>
            {logs.map((log, i) => (
              <tr key={i}>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.timestamp}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.user}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.action}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.resource}</td>
                <td style={{ padding: '12px', borderBottom: '1px solid #e2e6ec' }}>{log.result}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}