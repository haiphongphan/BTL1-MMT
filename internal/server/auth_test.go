package server

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Đăng ký: kiểm tra luật tên/mật khẩu, tên trùng, 20 người đăng ký cùng lúc không ghi đè nhau và
// users.json đọc lại được (server khởi động lại vẫn đăng nhập được).
func TestRegisterValidatesAndPersists(t *testing.T) {
	dir := t.TempDir()
	uf := filepath.Join(dir, "users.json")
	os.WriteFile(uf, []byte(`{}`), 0o644)
	s, err := NewServer(uf, 0)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.register(fmt.Sprintf("usr%d", i), "secret1")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		user, pass string
		want       error
	}{
		{"usr7", "secret1", errUserExists},
		{"ab", "secret1", errBadUsername},
		{"bad name", "secret1", errBadUsername},
		{"tên_có_dấu", "secret1", errBadUsername},
		{"okname", "12345", errBadPassword},
	}
	for _, c := range cases {
		if err := s.register(c.user, c.pass); err != c.want {
			t.Errorf("register(%q, %q) = %v, muốn %v", c.user, c.pass, err, c.want)
		}
	}

	s2, err := NewServer(uf, 0) // đọc lại file như khi server khởi động lại
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.users) != 20 {
		t.Fatalf("users.json có %d tài khoản, muốn 20", len(s2.users))
	}
	if !s2.checkPassword("usr13", "secret1") || s2.checkPassword("usr13", "wrong!") {
		t.Fatal("checkPassword sai với tài khoản vừa đăng ký")
	}
	if left, _ := filepath.Glob(filepath.Join(dir, ".users-*")); len(left) != 0 {
		t.Fatalf("còn file tạm: %v", left)
	}
}
