package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type copilotRefreshResult struct {
	s   copilotSession
	err error
}

func copilotRefreshModes(t *testing.T, test func(*testing.T, func(context.Context, copilotApp) (copilotSession, error))) {
	t.Helper()
	for _, mode := range []struct {
		name    string
		refresh func(context.Context, copilotApp) (copilotSession, error)
	}{{"editor", copilotToken}, {"cli", copilotDirect}} {
		t.Run(mode.name, func(t *testing.T) {
			resetCopilotState()
			t.Cleanup(resetCopilotState)
			synctest.Test(t, func(t *testing.T) { test(t, mode.refresh) })
		})
	}
}

func timeoutCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

func copilotRefreshTransport(t *testing.T, f func(*http.Request) (*http.Response, error)) {
	t.Helper()
	old := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: openCodeTransport(f)}
	t.Cleanup(func() { http.DefaultClient = old })
}

func copilotRefreshReply(r *http.Request) *http.Response {
	body, _ := json.Marshal(map[string]any{
		"token":      "session-" + strings.TrimPrefix(r.Header.Get("Authorization"), "token "),
		"expires_at": time.Now().Add(time.Hour).Unix(),
		"endpoints":  map[string]string{"api": "https://api.githubcopilot.com"},
	})
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: r}
}

// Hold a refresh at HTTP, then require both kinds of reader to finish on
// other accounts before releasing it. No wall-clock speed decides progress.
func TestCopilotRefreshLeavesOtherAccountsReadable(t *testing.T) {
	copilotRefreshModes(t, func(t *testing.T, refresh func(context.Context, copilotApp) (copilotSession, error)) {
		copilotSessions["cached"] = copilotSession{Token: "cached-session", ExpiresAt: time.Now().Add(time.Hour).Unix()}
		release := make(chan struct{})
		copilotRefreshTransport(t, func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") == "token held" {
				<-release
			}
			return copilotRefreshReply(r), nil
		})
		held := make(chan error, 1)
		go func() { _, err := refresh(t.Context(), copilotApp{Token: "held"}); held <- err }()
		synctest.Wait()
		defer func() { close(release); synctest.Wait(); <-held }()
		for _, reader := range []struct {
			name string
			call func(context.Context, copilotApp) (copilotSession, error)
		}{{"editor", copilotToken}, {"cli", copilotDirect}} {
			for _, token := range []string{"cached", "new-" + reader.name} {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				done := make(chan copilotRefreshResult, 1)
				go func() {
					s, err := reader.call(ctx, copilotApp{Token: token})
					done <- copilotRefreshResult{s, err}
				}()
				synctest.Wait()
				select {
				case got := <-done:
					if got.err != nil || got.s.Token == "" {
						t.Errorf("%s %s returned no session: %v", reader.name, token, got.err)
					}
				case <-ctx.Done():
					t.Fatalf("%s account %s waited for another account's held refresh", reader.name, token)
				}
				cancel()
				synctest.Wait()
			}
		}
	})
}

func TestCopilotRefreshSharesResult(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			copilotRefreshModes(t, func(t *testing.T, refresh func(context.Context, copilotApp) (copilotSession, error)) {
				release := make(chan struct{})
				var calls atomic.Int32
				copilotRefreshTransport(t, func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					<-release
					res := copilotRefreshReply(r)
					res.StatusCode = status
					return res, nil
				})
				done := make(chan copilotRefreshResult, 16)
				for range 16 {
					go func() {
						s, err := refresh(timeoutCtx(t, 2*time.Minute), copilotApp{Token: "shared"})
						done <- copilotRefreshResult{s, err}
					}()
				}
				synctest.Wait()
				if n := calls.Load(); n != 1 {
					t.Errorf("concurrent exchanges = %d, want 1", n)
				}
				close(release)
				synctest.Wait()
				for range 16 {
					got := <-done
					if (got.err == nil) != (status == http.StatusOK) {
						t.Errorf("status %d: %v", status, got.err)
					}
				}
				if n := calls.Load(); n != 1 {
					t.Errorf("shared result used %d exchanges, want 1", n)
				}
				next := make(chan error, 1)
				go func() { _, err := refresh(timeoutCtx(t, 2*time.Minute), copilotApp{Token: "shared"}); next <- err }()
				synctest.Wait()
				select {
				case err := <-next:
					if (err == nil) != (status == http.StatusOK) {
						t.Errorf("status %d, next call: %v", status, err)
					}
				case <-time.After(5 * time.Second):
					t.Error("next call never finished")
				}
				want := int32(1)
				if status != http.StatusOK {
					want = 2
				}
				if n := calls.Load(); n != want {
					t.Errorf("next call used %d exchanges, want %d", n, want)
				}
			})
		})
	}
}

func TestCopilotRefreshCancellationIsPerCaller(t *testing.T) {
	copilotRefreshModes(t, func(t *testing.T, refresh func(context.Context, copilotApp) (copilotSession, error)) {
		release := make(chan struct{})
		var calls atomic.Int32
		type proxyKey struct{}
		copilotRefreshTransport(t, func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			if r.Context().Value(proxyKey{}) != "account-proxy" {
				t.Error("refresh lost caller's context values")
			}
			select {
			case <-release:
				return copilotRefreshReply(r), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})
		ctx, cancel := context.WithCancel(context.WithValue(t.Context(), proxyKey{}, "account-proxy"))
		defer cancel()
		leader, follower, kept := make(chan error, 1), make(chan error, 1), make(chan error, 1)
		go func() { _, err := refresh(ctx, copilotApp{Token: "shared"}); leader <- err }()
		synctest.Wait()
		waitCtx, stop := context.WithCancel(ctx)
		defer stop()
		go func() { _, err := refresh(waitCtx, copilotApp{Token: "shared"}); follower <- err }()
		go func() { _, err := refresh(t.Context(), copilotApp{Token: "shared"}); kept <- err }()
		synctest.Wait()
		stop()
		synctest.Wait()
		select {
		case err := <-follower:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("canceled follower: %v", err)
			}
		default:
			t.Error("canceled follower is waiting for HTTP")
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-leader:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("canceled initiator: %v", err)
			}
		default:
			t.Error("canceled initiator is waiting for HTTP")
		}
		close(release)
		synctest.Wait()
		if err := <-kept; err != nil {
			t.Errorf("another caller's cancellation failed the remaining caller: %v", err)
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("cancellation restarted the shared exchange: %d calls", n)
		}
	})
}

func TestCopilotRefreshHasDeadline(t *testing.T) {
	copilotRefreshModes(t, func(t *testing.T, refresh func(context.Context, copilotApp) (copilotSession, error)) {
		var calls atomic.Int32
		copilotRefreshTransport(t, func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			deadline, ok := r.Context().Deadline()
			if !ok {
				t.Error("refresh has no deadline")
				return nil, context.DeadlineExceeded
			}
			if left := time.Until(deadline); left <= 0 || left > time.Minute {
				t.Errorf("refresh deadline is %v away", left)
			}
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
		if _, err := refresh(timeoutCtx(t, time.Minute), copilotApp{Token: "timed-out"}); err == nil {
			t.Error("timed-out exchange returned a session")
		}
		synctest.Wait()
		if n := calls.Load(); n != 1 {
			t.Errorf("timed-out exchange used %d calls", n)
		}
	})
}
