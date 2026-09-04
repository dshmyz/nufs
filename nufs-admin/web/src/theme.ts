const KEY = 'nufs-theme'

export function initTheme(): 'light' | 'dark' {
  const theme = localStorage.getItem(KEY) === 'light' ? 'light' : 'dark'
  document.documentElement.dataset.theme = theme
  return theme
}

export function toggleTheme(): 'light' | 'dark' {
  const next = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light'
  document.documentElement.dataset.theme = next
  localStorage.setItem(KEY, next)
  return next
}
