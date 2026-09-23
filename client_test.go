package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestRedactedBrokerURL(t *testing.T) {
	cases := []struct {
		name     string
		opts     []ClientOption
		want     string
		mustHide string
	}{
		{
			name: "비밀번호를 지운다",
			opts: []ClientOption{
				WithUsername("measurement-consumer"),
				WithPassword("s3cr3tP4ss"),
				WithHost("rabbitmq.message.svc.cluster.local"),
				WithPort("5672"),
			},
			want:     "amqp://measurement-consumer:xxxxx@rabbitmq.message.svc.cluster.local:5672/",
			mustHide: "s3cr3tP4ss",
		},
		{
			name: "비밀번호에 @ 나 : 가 있어도 지운다",
			opts: []ClientOption{
				WithUsername("svc"),
				WithPassword("p@ss:word"),
				WithHost("broker"),
				WithPort("5672"),
			},
			want:     "amqp://svc:xxxxx@broker:5672/",
			mustHide: "p@ss:word",
		},
		{
			// 기본값이 guest/guest 다. 비밀번호가 사용자명과 같다고 해서
			// 멀쩡히 가려진 URL 을 버리면 안 된다.
			name:     "비밀번호가 사용자명과 같아도 URL 을 살린다",
			opts:     []ClientOption{WithHost("127.0.0.1"), WithPort("1")},
			want:     "amqp://guest:xxxxx@127.0.0.1:1/",
			mustHide: "",
		},
		{
			name: "userinfo 밖에 있으면 통째로 버린다",
			opts: []ClientOption{
				WithURL("amqp://broker:5672/?auth=hunter2"),
				WithPassword("hunter2"),
			},
			want:     "(redacted broker url)",
			mustHide: "hunter2",
		},
		{
			name: "URL 로 파싱되지 않으면 버린다",
			opts: []ClientOption{
				WithUsername("svc"),
				WithPassword("pa/ss"),
				WithHost("broker"),
				WithPort("5672"),
			},
			want:     "(unparsable broker url)",
			mustHide: "pa/ss",
		},
		{
			name:     "URL 을 직접 준 경우에도 지운다",
			opts:     []ClientOption{WithURL("amqp://u:hunter2@broker:5672/vhost")},
			want:     "amqp://u:xxxxx@broker:5672/vhost",
			mustHide: "hunter2",
		},
		{
			name:     "비밀번호가 없으면 그대로 둔다",
			opts:     []ClientOption{WithURL("amqp://broker:5672/")},
			want:     "amqp://broker:5672/",
			mustHide: "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := NewClient(c.opts...).redactedBrokerURL()

			if c.mustHide != "" && strings.Contains(got, c.mustHide) {
				t.Fatalf("비밀번호가 남았다: %s", got)
			}

			if got != c.want {
				t.Fatalf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}

// 접속 로그가 실제로 마스킹된 값을 쓰는지, 주입한 로거로 확인한다.
func TestConnectLogsMaskedURL(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	c := NewClient(
		WithLogger(logger),
		WithUsername("svc"),
		WithPassword("s3cr3tP4ss"),
		// 닿지 않는 포트라 Dial 이 곧바로 거절된다.
		WithHost("127.0.0.1"),
		WithPort("1"),
	)

	// 접속은 실패해도 된다. 그 전에 찍는 줄만 본다.
	_ = c.connect(context.Background())

	out := buf.String()
	if strings.Contains(out, "s3cr3tP4ss") {
		t.Fatalf("로그에 비밀번호가 남았다:\n%s", out)
	}

	first, _, _ := strings.Cut(out, "\n")

	var m map[string]any
	if err := json.Unmarshal([]byte(first), &m); err != nil {
		t.Fatalf("JSON 이 아니다: %s", first)
	}

	if m["msg"] != "Attempting to connect to the broker" {
		t.Fatalf("첫 줄이 접속 로그가 아니다: %s", first)
	}

	if got, _ := m["broker_url"].(string); got != "amqp://svc:xxxxx@127.0.0.1:1/" {
		t.Fatalf("broker_url = %s", got)
	}
}

// 표준 log/slog 의 Error 는 (msg, args...) 다. err 을 그대로 넘기면 !BADKEY 로
// 찍혀 ECK 에서 error 필드로 색인되지 않는다.
func TestErrorLogsUseErrorKey(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	c := NewClient(
		WithLogger(logger),
		WithHost("127.0.0.1"),
		WithPort("1"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 재시도 없이 곧바로 빠져나오게 한다.

	_ = c.Connect(ctx)

	if strings.Contains(buf.String(), "BADKEY") {
		t.Fatalf("!BADKEY 가 찍혔다:\n%s", buf.String())
	}
}
