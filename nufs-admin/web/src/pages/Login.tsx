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
      minHeight: '100vh',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      background: '#f5f6f8',
    }}>
      <form
        onSubmit={handleSubmit}
        style={{
          background: '#fff',
          padding: '32px',
          borderRadius: '12px',
          border: '1px solid #e2e6ec',
          width: '320px',
        }}
      >
        <h2 style={{ fontSize: '20px', fontWeight: 700, marginBottom: '24px', textAlign: 'center' }}>
          NUFS Admin 登录
        </h2>

        <div style={{ marginBottom: '16px' }}>
          <label style={{ fontSize: '13px', color: '#5a6478', marginBottom: '6px', display: 'block' }}>
            用户名
          </label>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            style={{
              width: '100%',
              padding: '10px 12px',
              border: '1px solid #e2e6ec',
              borderRadius: '6px',
              fontSize: '14px',
            }}
            autoFocus
          />
        </div>

        <div style={{ marginBottom: '24px' }}>
          <label style={{ fontSize: '13px', color: '#5a6478', marginBottom: '6px', display: 'block' }}>
            密码
          </label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            style={{
              width: '100%',
              padding: '10px 12px',
              border: '1px solid #e2e6ec',
              borderRadius: '6px',
              fontSize: '14px',
            }}
          />
        </div>

        {error && (
          <div style={{
            padding: '10px',
            background: '#fee2e2',
            color: '#dc2626',
            borderRadius: '6px',
            marginBottom: '16px',
            fontSize: '13px',
          }}>
            {error}
          </div>
        )}

        <button
          type="submit"
          style={{
            width: '100%',
            padding: '12px',
            background: '#2563eb',
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            fontSize: '14px',
            fontWeight: 600,
            cursor: 'pointer',
          }}
        >
          登录
        </button>
      </form>
    </div>
  )
}