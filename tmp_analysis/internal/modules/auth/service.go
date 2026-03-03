package auth

import (
	"context"
	"fmt"
	"time"

	"firebase.google.com/go/v4/auth"
	wh "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/google/uuid"
	"github.com/ristocalldevs/backend/internal/models"
	fb "github.com/ristocalldevs/backend/internal/pkg/firebase"
)

type RegistrationRequest struct {
	ExternalUserId    string    `json:"external_user_id"`
	ExternalSessionId string    `json:"external_session_id"`
	Name              string    `json:"name"`
	Surname           string    `json:"surname"`
	Birthday          time.Time `json:"birthday"`
	TosAccepted       bool      `json:"tos_accepted"`
	PrivacyAccepted   bool      `json:"privacy_accepted"`
}

type AuthService interface {
	Register(ctx context.Context, req RegistrationRequest) (*models.User, error)
	IsEmailAvailable(ctx context.Context, email string) (bool, error)
	ChangePassword(ctx context.Context, uid string, newPassword string) error
	Logout(ctx context.Context, uid string, sessionID string) error
	CreateSessionCookie(ctx context.Context, idToken string, expiresIn time.Duration) (string, error)
	VerifyIDToken(ctx context.Context, idToken string) (*auth.Token, error)
	GetUser(ctx context.Context, uid string) (*auth.UserRecord, error)
}

type authService struct {
	db       *wh.DbContext
	firebase *fb.Client
}

func NewAuthService(db *wh.DbContext, firebase *fb.Client) AuthService {
	return &authService{db: db, firebase: firebase}
}

func (s *authService) CreateSessionCookie(ctx context.Context, idToken string, expiresIn time.Duration) (string, error) {
	return s.firebase.Auth.SessionCookie(ctx, idToken, expiresIn)
}

func (s *authService) VerifyIDToken(ctx context.Context, idToken string) (*auth.Token, error) {
	return s.firebase.Auth.VerifyIDToken(ctx, idToken)
}

func (s *authService) GetUser(ctx context.Context, uid string) (*auth.UserRecord, error) {
	return s.firebase.Auth.GetUser(ctx, uid)
}

func (s *authService) ChangePassword(ctx context.Context, uid string, newPassword string) error {
	params := (&auth.UserToUpdate{}).Password(newPassword)
	_, err := s.firebase.Auth.UpdateUser(ctx, uid, params)
	return err
}

func (s *authService) Logout(ctx context.Context, uid string, sessionID string) error {
	return s.firebase.Auth.RevokeRefreshTokens(ctx, uid)
}

func (s *authService) Register(ctx context.Context, req RegistrationRequest) (*models.User, error) {
	fUser, err := s.firebase.Auth.GetUser(ctx, req.ExternalUserId)
	if err != nil {
		return nil, fmt.Errorf("failed to get firebase user: %w", err)
	}

	user := &models.User{
		Id:              uuid.New().String(),
		ExternalId:      req.ExternalUserId,
		Name:            req.Name,
		Surname:         req.Surname,
		Email:           fUser.Email,
		Birthday:        req.Birthday,
		CreatedAt:       time.Now(),
		CountryCode:     "IT",
		Status:          models.StatusPending,
		ExperienceYears: 0,
		HasHaccp:        false,
		VatOwner:        false,
	}

	s.db.Add(user)
	
	// Add Acks
	tosStatus := models.StatusCanceled
	if req.TosAccepted {
		tosStatus = models.StatusActive
	}
	s.db.Add(&models.UserAck{
		Id:        uuid.New().String(),
		UserId:    user.Id,
		CreatedAt: time.Now(),
		Status:    tosStatus,
	})

	privStatus := models.StatusCanceled
	if req.PrivacyAccepted {
		privStatus = models.StatusActive
	}
	s.db.Add(&models.UserAck{
		Id:        uuid.New().String(),
		UserId:    user.Id,
		CreatedAt: time.Now(),
		Status:    privStatus,
	})

	if err := s.db.Save(); err != nil {
		return nil, fmt.Errorf("failed to save user: %w", err)
	}

	return user, nil
}

func (s *authService) IsEmailAvailable(ctx context.Context, email string) (bool, error) {
	var users []models.User
	u := &models.User{}
	err := s.db.Set(&users).
		Where(dsl.Eq(u, &u.Email, email)).
		All()
	if err != nil {
		return false, err
	}
	return len(users) == 0, nil
}
