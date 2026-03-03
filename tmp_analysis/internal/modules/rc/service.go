package rc

import (
	"context"
	"fmt"
	"time"

	wh "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/ristocalldevs/backend/internal/models"
)

type JobOfferWithCount struct {
	models.JobOffer
	ApplicantsCount uint16 `json:"applicants_count"`
	Owner           *models.POS `json:"owner,omitempty"`
}

type RCService interface {
	GetAvailableJobs(ctx context.Context, offset, limit int, country, province string) ([]JobOfferWithCount, error)
	GetJobDetails(ctx context.Context, id int64, includeOwner bool) (*JobOfferWithCount, error)
	ApplyToJob(ctx context.Context, userId string, jobId int64, coverLetter string) (*models.JobCandidate, error)
	GetMyApplications(ctx context.Context, userId string) ([]models.JobCandidate, error)
	GetMyJobOffers(ctx context.Context, userId string) ([]models.JobOffer, error)
	CreateJobOffer(ctx context.Context, offer models.JobOffer) (*models.JobOffer, error)
	GetPOS(ctx context.Context, id int64) (*models.POS, error)
	GetMyPOSes(ctx context.Context, ownerId string) ([]models.POS, error)
	CreatePOS(ctx context.Context, pos models.POS) (*models.POS, error)
	GetLegalEntity(ctx context.Context, id int64) (*models.LegalEntity, error)
	GetMyLegalEntities(ctx context.Context, ownerId string) ([]models.LegalEntity, error)
	CreateLegalEntity(ctx context.Context, entity models.LegalEntity) (*models.LegalEntity, error)
	GetAvailableCourses(ctx context.Context) ([]models.Course, error)
	GetCourse(ctx context.Context, id int16) (*models.Course, error)
	GetMe(ctx context.Context, uid string) (*models.User, error)
	UpdateMe(ctx context.Context, user models.User) error
}

type rcService struct {
	db *wh.DbContext
}

func NewRCService(db *wh.DbContext) RCService {
	return &rcService{db: db}
}

func (s *rcService) GetAvailableJobs(ctx context.Context, offset, limit int, country, province string) ([]JobOfferWithCount, error) {
	var offers []models.JobOffer
	o := &models.JobOffer{}
	
	q := s.db.Set(&offers).
		Where(dsl.Eq(o, &o.Status, 1))

	if limit > 0 {
		q.Limit(limit).Offset(offset)
	}

	if err := q.All(); err != nil {
		return nil, err
	}

	var result []JobOfferWithCount
	for _, off := range offers {
		var candidates []models.JobCandidate
		c := &models.JobCandidate{}
		s.db.Set(&candidates).
			Where(dsl.Eq(c, &c.OfferId, off.Id), dsl.Eq(c, &c.Status, 1)).
			All()

		var pos models.POS
		s.db.Set(&pos).Find(off.OwnerId)

		result = append(result, JobOfferWithCount{
			JobOffer:        off,
			ApplicantsCount: uint16(len(candidates)),
			Owner:           &pos,
		})
	}

	return result, nil
}

func (s *rcService) GetJobDetails(ctx context.Context, id int64, includeOwner bool) (*JobOfferWithCount, error) {
	var offer models.JobOffer
	if err := s.db.Set(&offer).Find(id); err != nil {
		return nil, err
	}

	res := &JobOfferWithCount{JobOffer: offer}

	var candidates []models.JobCandidate
	c := &models.JobCandidate{}
	s.db.Set(&candidates).
		Where(dsl.Eq(c, &c.OfferId, offer.Id), dsl.Eq(c, &c.Status, 1)).
		All()
	res.ApplicantsCount = uint16(len(candidates))

	if includeOwner {
		var pos models.POS
		s.db.Set(&pos).Find(offer.OwnerId)
		res.Owner = &pos
	}

	return res, nil
}

func (s *rcService) ApplyToJob(ctx context.Context, externalUserId string, jobId int64, coverLetter string) (*models.JobCandidate, error) {
	var users []models.User
	u := &models.User{}
	s.db.Set(&users).Where(dsl.Eq(u, &u.ExternalId, externalUserId)).Limit(1).All()
	if len(users) == 0 {
		return nil, fmt.Errorf("user not found")
	}

	candidate := &models.JobCandidate{
		UserId:      users[0].Id,
		OfferId:     jobId,
		CreatedAt:   time.Now(),
		CoverLetter: coverLetter,
		Status:      1,
	}

	s.db.Add(candidate)
	if err := s.db.Save(); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (s *rcService) GetMyApplications(ctx context.Context, externalUserId string) ([]models.JobCandidate, error) {
	var users []models.User
	u := &models.User{}
	s.db.Set(&users).Where(dsl.Eq(u, &u.ExternalId, externalUserId)).Limit(1).All()
	if len(users) == 0 {
		return nil, fmt.Errorf("user not found")
	}

	var apps []models.JobCandidate
	c := &models.JobCandidate{}
	err := s.db.Set(&apps).
		Where(dsl.Eq(c, &c.UserId, users[0].Id)).
		All()
	return apps, err
}

func (s *rcService) GetMyJobOffers(ctx context.Context, externalUserId string) ([]models.JobOffer, error) {
	var users []models.User
	u := &models.User{}
	s.db.Set(&users).Where(dsl.Eq(u, &u.ExternalId, externalUserId)).Limit(1).All()
	if len(users) == 0 {
		return nil, fmt.Errorf("user not found")
	}

	var poses []models.POS
	p := &models.POS{}
	s.db.Set(&poses).Where(dsl.Eq(p, &p.OwnerId, &users[0].Id)).All()

	var result []models.JobOffer
	for _, pos := range poses {
		var offers []models.JobOffer
		o := &models.JobOffer{}
		s.db.Set(&offers).Where(dsl.Eq(o, &o.OwnerId, pos.Id)).All()
		result = append(result, offers...)
	}

	return result, nil
}

func (s *rcService) CreateJobOffer(ctx context.Context, offer models.JobOffer) (*models.JobOffer, error) {
	offer.CreatedAt = time.Now()
	if offer.Status == 0 {
		offer.Status = 1
	}
	s.db.Add(&offer)
	if err := s.db.Save(); err != nil {
		return nil, err
	}
	return &offer, nil
}

func (s *rcService) GetPOS(ctx context.Context, id int64) (*models.POS, error) {
	var pos models.POS
	if err := s.db.Set(&pos).Find(id); err != nil {
		return nil, err
	}
	return &pos, nil
}

func (s *rcService) GetMyPOSes(ctx context.Context, ownerId string) ([]models.POS, error) {
	var poses []models.POS
	p := &models.POS{}
	err := s.db.Set(&poses).
		Where(dsl.Eq(p, &p.OwnerId, &ownerId)).
		All()
	return poses, err
}

func (s *rcService) CreatePOS(ctx context.Context, pos models.POS) (*models.POS, error) {
	s.db.Add(&pos)
	if err := s.db.Save(); err != nil {
		return nil, err
	}
	return &pos, nil
}

func (s *rcService) GetLegalEntity(ctx context.Context, id int64) (*models.LegalEntity, error) {
	var entity models.LegalEntity
	if err := s.db.Set(&entity).Find(id); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (s *rcService) GetMyLegalEntities(ctx context.Context, ownerId string) ([]models.LegalEntity, error) {
	var entities []models.LegalEntity
	e := &models.LegalEntity{}
	err := s.db.Set(&entities).
		Where(dsl.Eq(e, &e.OwnerId, ownerId)).
		All()
	return entities, err
}

func (s *rcService) CreateLegalEntity(ctx context.Context, entity models.LegalEntity) (*models.LegalEntity, error) {
	s.db.Add(&entity)
	if err := s.db.Save(); err != nil {
		return nil, err
	}
	return &entity, nil
}

func (s *rcService) GetAvailableCourses(ctx context.Context) ([]models.Course, error) {
	var courses []models.Course
	c := &models.Course{}
	err := s.db.Set(&courses).
		Where(dsl.Eq(c, &c.Status, 1)).
		All()
	return courses, err
}

func (s *rcService) GetCourse(ctx context.Context, id int16) (*models.Course, error) {
	var course models.Course
	if err := s.db.Set(&course).Find(id); err != nil {
		return nil, err
	}
	return &course, nil
}

func (s *rcService) GetMe(ctx context.Context, uid string) (*models.User, error) {
	var users []models.User
	u := &models.User{}
	err := s.db.Set(&users).
		Where(dsl.Eq(u, &u.ExternalId, uid)).
		Limit(1).
		All()
	if err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("user not found")
	}
	return &users[0], nil
}

func (s *rcService) UpdateMe(ctx context.Context, user models.User) error {
	var users []models.User
	u := &models.User{}
	err := s.db.Set(&users).
		Where(dsl.Eq(u, &u.ExternalId, user.ExternalId)).
		Limit(1).
		All()
	if err != nil || len(users) == 0 {
		return fmt.Errorf("user not found")
	}
	
	dbUser := &users[0]
	s.db.Attach(dbUser)
	
	dbUser.Name = user.Name
	dbUser.Surname = user.Surname
	dbUser.Phone = user.Phone
	dbUser.Bio = user.Bio
	dbUser.Address = user.Address
	dbUser.Birthday = user.Birthday
	
	return s.db.Save()
}
