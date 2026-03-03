package models

import (
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
)

func RegisterAll() {
	dsl.Register(User{})
	dsl.Register(UserAck{})
	dsl.Register(PromoCode{})
	dsl.Register(Service{})
	dsl.Register(StripeRecord{})
	dsl.Register(Transaction{})
	dsl.Register(UserSubscription{})
	dsl.Register(Adv{})
	dsl.Register(Badge{})
	dsl.Register(Course{})
	dsl.Register(Instructor{})
	dsl.Register(JobOffer{})
	dsl.Register(LegalEntity{})
	dsl.Register(POS{})
	dsl.Register(JobCandidate{})
	dsl.Register(Chat{})
	dsl.Register(ChatMessage{})
	dsl.Register(Notification{})
	dsl.Register(UserDevice{})
	dsl.Register(EmailLog{})
	dsl.Register(NotificationLog{})
	dsl.Register(SystemLog{})
	dsl.Register(FeaturesToggle{})
	dsl.Register(Setting{})
}
