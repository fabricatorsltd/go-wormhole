package middleware

import (
	"context"
	"net/http"

	fb "github.com/ristocalldevs/backend/internal/pkg/firebase"
)

func FirebaseAuth(client *fb.Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie("session")
			if err != nil {
				http.Error(w, "Unauthorized: missing session cookie", http.StatusUnauthorized)
				return
			}

			// Verify session cookie with Firebase
			decodedToken, err := client.Auth.VerifySessionCookieAndCheckRevoked(r.Context(), cookie.Value)
			if err != nil {
				http.Error(w, "Unauthorized: invalid session cookie", http.StatusUnauthorized)
				return
			}

			// Inject UID into context
			ctx := context.WithValue(r.Context(), "fUserId", decodedToken.UID)

			// Also fetch and inject full UserRecord if needed (matching C# behavior)
			fUser, _ := client.Auth.GetUser(ctx, decodedToken.UID)
			if fUser != nil {
				ctx = context.WithValue(ctx, "firebaseUser", fUser)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
