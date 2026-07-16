// Package auth provides JWT-based authentication.
package auth

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

var ErrInvalidCredentials = errors.New("invalid credentials")

// User represents a stored user with bcrypt-hashed password.
type User struct {
	Username     string `yaml:"username"`
	PasswordHash string `yaml:"password_hash"`
}

// UserStore manages user credentials.
type UserStore struct {
	users map[string]string // username → password hash
	mu    sync.RWMutex
}

// LoadUsers reads users from YAML file.
func LoadUsers(path string) (*UserStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var userList []User
	if err := yaml.Unmarshal(data, &userList); err != nil {
		return nil, err
	}

	users := make(map[string]string)
	for _, u := range userList {
		users[u.Username] = u.PasswordHash
	}

	return &UserStore{users: users}, nil
}

// Authenticate validates username and password.
func (s *UserStore) Authenticate(username, password string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, ok := s.users[username]
	if !ok {
		return ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return ErrInvalidCredentials
	}

	return nil
}

// Exists checks if a user exists.
func (s *UserStore) Exists(username string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.users[username] != ""
}