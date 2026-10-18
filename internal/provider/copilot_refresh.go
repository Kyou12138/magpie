package provider

import (
	"context"
	"time"
)

// copilotRefresh is one GitHub token's exchange for a Copilot session, on
// the wire or just finished: the first caller to need it runs it, and every
// caller that arrives while it runs waits for the same answer.
type copilotRefresh struct {
	done chan struct{}
	s    copilotSession
	err  error
}

// copilotRefreshes is the exchange each GitHub token has going, outside the
// session cache's lock: one account's refresh on the wire leaves every
// other account free to read its session or start its own.
var copilotRefreshes = map[string]*copilotRefresh{}

// cachedCopilot is copilotToken and copilotDirect's shared half: the cached
// session while it has more than two minutes to live, else one exchange for
// the token that every caller of that token shares. The exchange keeps the
// first caller's context values (an account's proxy among them) but not its
// cancellation, and is bounded by its own deadline, so a caller leaving
// never cancels or stalls another's.
func cachedCopilot(ctx context.Context, token string, fetch func(context.Context) (copilotSession, error)) (copilotSession, error) {
	if err := ctx.Err(); err != nil {
		return copilotSession{}, err
	}
	copilotMu.Lock()
	if s, ok := copilotSessions[token]; ok && time.Until(time.Unix(s.ExpiresAt, 0)) > 2*time.Minute {
		copilotMu.Unlock()
		return s, nil
	}
	r := copilotRefreshes[token]
	if r == nil {
		r = &copilotRefresh{done: make(chan struct{})}
		copilotRefreshes[token] = r
		go func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			r.s, r.err = fetch(ctx)
			copilotMu.Lock()
			// a reset meanwhile dropped this exchange's claim: its answer
			// still reaches its waiters, but nothing is cached from it
			if copilotRefreshes[token] == r {
				delete(copilotRefreshes, token)
				if r.err == nil {
					copilotSessions[token] = r.s
				}
			}
			close(r.done)
			copilotMu.Unlock()
		}()
	}
	copilotMu.Unlock()
	select {
	case <-r.done:
		return r.s, r.err
	case <-ctx.Done():
		return copilotSession{}, ctx.Err()
	}
}
