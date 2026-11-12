// mkreport chuyển docs/report.md thành docs/report.html để xuất PDF bằng trình duyệt (Ctrl+P → Save as PDF).
// Trình duyệt tự render Markdown (marked) và sơ đồ Mermaid (mermaid), nên không cần cài pandoc/LaTeX.
//
//	go run ./scripts/mkreport              # docs/report.md → docs/report.html
//	go run ./scripts/mkreport in.md out.html
package main

import (
	"log"
	"os"
	"strings"
)

const page = `<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Báo cáo BTL1</title>
<style>
  :root { --fg: #1a1a1a; --muted: #555; --line: #d0d0d0; --code: #f4f4f4; }
  body { background: #fff; color: var(--fg); font: 11pt/1.55 "Segoe UI", "Noto Sans", Arial, sans-serif;
         max-width: 900px; margin: 0 auto; padding: 24px 16px; }
  h1 { font-size: 20pt; border-bottom: 2px solid var(--fg); padding-bottom: 6px; }
  h2 { font-size: 15pt; margin-top: 1.8em; border-bottom: 1px solid var(--line); padding-bottom: 4px; }
  h3 { font-size: 12.5pt; margin-top: 1.4em; }
  table { border-collapse: collapse; width: 100%; margin: .8em 0; font-size: 10pt; }
  th, td { border: 1px solid var(--line); padding: 4px 7px; text-align: left; vertical-align: top; }
  th { background: var(--code); }
  code { background: var(--code); padding: 1px 4px; border-radius: 3px; font-size: 9.5pt; }
  pre { background: var(--code); padding: 10px; overflow-x: auto; font-size: 9pt; line-height: 1.35; }
  pre code { background: none; padding: 0; }
  blockquote { border-left: 4px solid #e0a800; background: #fff8e0; margin: 1em 0; padding: 6px 12px; }
  img { max-width: 100%; }
  .mermaid { text-align: center; margin: 1em 0; }
  @media print {
    body { max-width: none; padding: 0; }
    blockquote { display: none; }           /* ghi chú "bản nháp" không in */
    h2 { break-after: avoid; }
    table, pre, .mermaid, img { break-inside: avoid; }
  }
</style>
</head>
<body>
<div id="content">Đang tải… (cần Internet để tải marked và mermaid)</div>
<script type="text/plain" id="src">{{MARKDOWN}}</script>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
<script>
  const md = document.getElementById('src').textContent.replaceAll('<\\/script', '</script');
  const el = document.getElementById('content');
  el.innerHTML = marked.parse(md);
  el.querySelectorAll('pre code.language-mermaid').forEach(code => {
    const div = document.createElement('div');
    div.className = 'mermaid';
    div.textContent = code.textContent;
    code.parentElement.replaceWith(div);
  });
  mermaid.initialize({ startOnLoad: false, theme: 'neutral', securityLevel: 'loose' });
  mermaid.run();
</script>
</body>
</html>
`

func main() {
	in, out := "docs/report.md", "docs/report.html"
	if len(os.Args) == 3 {
		in, out = os.Args[1], os.Args[2]
	}
	b, err := os.ReadFile(in)
	if err != nil {
		log.Fatal(err)
	}
	md := strings.ReplaceAll(string(b), "</script", `<\/script`) // không để Markdown đóng thẻ <script> sớm
	if err := os.WriteFile(out, []byte(strings.Replace(page, "{{MARKDOWN}}", md, 1)), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("đã ghi %s — mở bằng trình duyệt, Ctrl+P → Save as PDF", out)
}
