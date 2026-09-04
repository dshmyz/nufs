import { useState } from 'react'
import { login } from '../api/client'

interface LoginProps {
  onLogin: (token: string) => void
}

export default function Login({ onLogin }: LoginProps) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    try {
      const token = await login(username, password)
      onLogin(token)
    } catch (err: any) {
      setError(err.response?.data?.error || '登录失败')
    }
  }

  return (
    <div style={{
      minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
      background: 'radial-gradient(1100px 480px at 50% -12%, #1b2a4d 0%, var(--bg) 58%), var(--bg)',
    }}>
      <form onSubmit={handleSubmit} className="panel panel-pad" style={{ width: 320 }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, marginBottom: 6 }}>
          <span style={{ color: 'var(--accent)', fontSize: 18 }}>▧</span>
          <h1 style={{ fontSize: 18 }}>NUFS Console</h1>
        </div>
        <div className="faint" style={{ fontSize: 12, marginBottom: 26 }}>分布式存储 · 多集群运维管理台</div>

        <div className="form-field">
          <label>用户名</label>
          <input type="text" value={username} onChange={e => setUsername(e.target.value)} autoFocus />
        </div>
        <div className="form-field">
          <label>密码</label>
          <input type="password" value={password} onChange={e => setPassword(e.target.value)} />
        </div>

        {error && (
          <div style={{
            padding: '9px 11px', background: 'var(--danger-dim)', color: 'var(--danger)',
            borderRadius: 'var(--radius-sm)', marginBottom: 14, fontSize: 13,
          }}>{error}</div>
        )}

        <button type="submit" className="btn btn-primary" style={{ width: '100%', justifyContent: 'center', padding: '10px' }}>
          登录
        </button>
      </form>
    </div>
  )
}
