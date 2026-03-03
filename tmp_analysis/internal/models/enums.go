package models

type OtpType int

const (
	OtpEmailConfirmation OtpType = 0
	OtpPasswordReset    OtpType = 1
	OtpLoginConfirmation OtpType = 2
)

type EntityStatus int

const (
	StatusExpired   EntityStatus = -2
	StatusCanceled  EntityStatus = -1
	StatusPending   EntityStatus = 0
	StatusActive    EntityStatus = 1
	StatusSuspended EntityStatus = 2
)

type ReactionType int

const (
	ReactionLike  ReactionType = 0
	ReactionLove  ReactionType = 1
	ReactionLaugh ReactionType = 2
)

type ServiceType int

const (
	ServiceCredit    ServiceType = 0
	ServiceOneTime   ServiceType = 1
	ServiceRecurring ServiceType = 2
)

type PromoCodeType int

const (
	PromoCredit PromoCodeType = 0
	PromoDays   PromoCodeType = 1
)

type ClosingReason int

const (
	ReasonFoundViaRistocall ClosingReason = 0
	ReasonFoundOutside      ClosingReason = 1
	ReasonNotLookingAnymore ClosingReason = 2
	ReasonNotAbleToFind     ClosingReason = 3
)

type UserRcUseReason string

const (
	ReasonFindProfessionals        UserRcUseReason = "FindProfessionals"
	ReasonFindJob                 UserRcUseReason = "FindJob"
	ReasonCommunicateWithProfessionals UserRcUseReason = "CommunicateWithProfessionals"
)
