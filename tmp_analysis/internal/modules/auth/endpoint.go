package auth

import (
	"context"
	"net/http"
	"time"

	"github.com/mirkobrombin/go-module-router/v2/pkg/core"
	"github.com/ristocalldevs/backend/internal/pkg/response"
)

// RegisterEndpoint handles POST /v1/auth/register
type RegisterEndpoint struct {
	Meta core.Pattern `method:"POST" path:"/v1/auth/register"`

	// Body auto-bound from JSON
	Body RegistrationRequest `body:"json"`

	// Service injected
	AuthService AuthService
}

func (e *RegisterEndpoint) Handle(ctx context.Context) (any, error) {
	user, err := e.AuthService.Register(ctx, e.Body)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusCreated, user), nil
}

// EmailAvailabilityEndpoint handles GET /v1/auth/UsrEmailAvailability/{email}
type EmailAvailabilityEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/auth/UsrEmailAvailability/{email}"`

	// Path parameter
	Email string `path:"email"`

	AuthService AuthService
}

func (e *EmailAvailabilityEndpoint) Handle(ctx context.Context) (any, error) {
	available, err := e.AuthService.IsEmailAvailable(ctx, e.Email)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	status := http.StatusOK
	if !available {
		status = http.StatusBadRequest
	}
	return response.New(status, available), nil
}

// SetSessionCookieEndpoint handles POST /v1/auth/session
type SetSessionCookieEndpoint struct {
	Meta core.Pattern `method:"POST" path:"/v1/auth/session"`

	AuthService AuthService
}

func (e *SetSessionCookieEndpoint) Handle(ctx context.Context) (any, error) {
	req, _ := ctx.Value("http_request").(*http.Request)
	w, _ := ctx.Value("http_response_writer").(http.ResponseWriter)

	sessionToken := req.Header.Get("X-FST")
	if sessionToken == "" {
		return response.Error[any](http.StatusBadRequest, "Missing required headers."), nil
	}

	_, err := e.AuthService.VerifyIDToken(ctx, sessionToken)
	if err != nil {
		return response.Error[any](http.StatusUnauthorized, "Invalid session token."), nil
	}

	expiresIn := time.Hour * 1
	sessionCookie, err := e.AuthService.CreateSessionCookie(ctx, sessionToken, expiresIn)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, "Failed to create session cookie."), nil
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session",
		Value:    sessionCookie,
		Path:     "/",
		MaxAge:   int(expiresIn.Seconds()),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteNoneMode,
	})

	// Note: We don't have an easy way to inject into HttpContext.Items for subsequent handlers in same request
	// but for this specific Created() response it is fine.
	
	w.WriteHeader(http.StatusCreated)
	return nil, nil
}

type ChangePasswordRequest struct {
	NewPassword string `json:"new_password"`
}

// ChangePasswordEndpoint handles POST /v1/auth/password
type ChangePasswordEndpoint struct {
	Meta core.Pattern `method:"POST" path:"/v1/auth/password"`

	Body ChangePasswordRequest `body:"json"`

	AuthService AuthService
}

func (e *ChangePasswordEndpoint) Handle(ctx context.Context) (any, error) {
	uid, ok := ctx.Value("fUserId").(string)
	if !ok {
		return response.Error[any](http.StatusUnauthorized, "missing user id"), nil
	}

	err := e.AuthService.ChangePassword(ctx, uid, e.Body.NewPassword)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, "password changed"), nil
}

// LogoutEndpoint handles DELETE /v1/auth
type LogoutEndpoint struct {
	Meta core.Pattern `method:"DELETE" path:"/v1/auth"`

	AuthService AuthService
}

func (e *LogoutEndpoint) Handle(ctx context.Context) (any, error) {
	uid, ok := ctx.Value("fUserId").(string)
	if !ok {
		return response.Error[any](http.StatusUnauthorized, "missing user id"), nil
	}

	// Session ID from cookie
	req, _ := ctx.Value("http_request").(*http.Request)
	session, _ := req.Cookie("session")
	sessionID := ""
	if session != nil {
		sessionID = session.Value
	}

	err := e.AuthService.Logout(ctx, uid, sessionID)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, "logged out"), nil
}
