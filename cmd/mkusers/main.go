// mkusers tạo/cập nhật users.json: mật khẩu được lưu dạng sha256(salt‖pass), salt ngẫu nhiên mỗi user.
//
//	go run ./cmd/mkusers -out users.json -pass 123456 alice bob
//	go run ./cmd/mkusers -out users.json -n 20          # user1..user20, dùng cho run_clients.sh
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
)

type account struct {
	Salt string `json:"salt"`
	Hash string `json:"hash"`
}

func main() {
	out := flag.String("out", "users.json", "File users.json để ghi (giữ lại các user đã có)")
	pass := flag.String("pass", "123456", "Mật khẩu cho các user được tạo")
	n := flag.Int("n", 0, "Tạo thêm user1..userN")
	flag.Parse()

	users := map[string]account{}
	if b, err := os.ReadFile(*out); err == nil {
		if err := json.Unmarshal(b, &users); err != nil {
			log.Fatalf("%s hỏng: %v", *out, err)
		}
	}

	names := flag.Args()
	for i := 1; i <= *n; i++ {
		names = append(names, fmt.Sprintf("user%d", i))
	}
	if len(names) == 0 {
		log.Fatal("cần tên user hoặc -n")
	}

	for _, name := range names {
		salt := make([]byte, 8)
		rand.Read(salt)
		s := hex.EncodeToString(salt)
		h := sha256.Sum256([]byte(s + *pass))
		users[name] = account{Salt: s, Hash: hex.EncodeToString(h[:])}
	}

	b, _ := json.MarshalIndent(users, "", "  ")
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Đã ghi %d user vào %s\n", len(users), *out)
}
