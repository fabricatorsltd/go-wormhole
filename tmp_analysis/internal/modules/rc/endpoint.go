package rc

import (
	"context"
	"net/http"

	"github.com/mirkobrombin/go-module-router/v2/pkg/core"
	"github.com/ristocalldevs/backend/internal/models"
	"github.com/ristocalldevs/backend/internal/pkg/response"
)

// AvailableJobsEndpoint handles GET /v1/RC/AvailableJobs
type AvailableJobsEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/RC/AvailableJobs"`

	Offset   int    `query:"offset"`
	Limit    int    `query:"limit" default:"10"`
	Country  string `query:"country"`
	Province string `query:"province"`

	RCService RCService
}

func (e *AvailableJobsEndpoint) Handle(ctx context.Context) (any, error) {
	jobs, err := e.RCService.GetAvailableJobs(ctx, e.Offset, e.Limit, e.Country, e.Province)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, err.Error()), nil
	}
	return response.New(http.StatusOK, jobs), nil
}

// CreateJobOfferEndpoint handles POST /v1/RC/JobOffer
type CreateJobOfferEndpoint struct {
	Meta core.Pattern `method:"POST" path:"/v1/RC/JobOffer"`

	Body models.JobOffer `body:"json"`

	RCService RCService
}

func (e *CreateJobOfferEndpoint) Handle(ctx context.Context) (any, error) {
	job, err := e.RCService.CreateJobOffer(ctx, e.Body)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusCreated, job), nil
}

// GetJobOfferDetailsEndpoint handles GET /v1/JobOffer/{id}/{includeOwner}
type GetJobOfferDetailsEndpoint struct {
	Meta         core.Pattern `method:"GET" path:"/v1/JobOffer/{id}/{includeOwner}"`
	Id           int64        `path:"id"`
	IncludeOwner bool         `path:"includeOwner"`

	RCService RCService
}

func (e *GetJobOfferDetailsEndpoint) Handle(ctx context.Context) (any, error) {
	job, err := e.RCService.GetJobDetails(ctx, e.Id, e.IncludeOwner)
	if err != nil {
		return response.Error[any](http.StatusNotFound, err.Error()), nil
	}
	return response.New(http.StatusOK, job), nil
}

// GetMyApplicationsEndpoint handles GET /v1/JobOffer/applications
type GetMyApplicationsEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/JobOffer/applications"`

	RCService RCService
}

func (e *GetMyApplicationsEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	apps, err := e.RCService.GetMyApplications(ctx, uid)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, err.Error()), nil
	}
	return response.New(http.StatusOK, apps), nil
}

// GetMyJobOffersEndpoint handles GET /v1/JobOffer/mine
type GetMyJobOffersEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/JobOffer/mine"`

	RCService RCService
}

func (e *GetMyJobOffersEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	offers, err := e.RCService.GetMyJobOffers(ctx, uid)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, err.Error()), nil
	}
	return response.New(http.StatusOK, offers), nil
}

// ApplyToJobEndpoint handles POST /v1/JobOffer/apply/{id}
type ApplyToJobEndpoint struct {
	Meta  core.Pattern `method:"POST" path:"/v1/JobOffer/apply/{id}"`
	Id    int64        `path:"id"`
	Body  string       `body:"json"`

	RCService RCService
}

func (e *ApplyToJobEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	res, err := e.RCService.ApplyToJob(ctx, uid, e.Id, e.Body)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, res), nil
}

// GetLegalEntityEndpoint handles GET /v1/LegalEntity/{id}
type GetLegalEntityEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/LegalEntity/{id}"`
	Id   int64        `path:"id"`

	RCService RCService
}

func (e *GetLegalEntityEndpoint) Handle(ctx context.Context) (any, error) {
	entity, err := e.RCService.GetLegalEntity(ctx, e.Id)
	if err != nil {
		return response.Error[any](http.StatusNotFound, err.Error()), nil
	}
	return response.New(http.StatusOK, entity), nil
}

// CreateLegalEntityEndpoint handles POST /v1/LegalEntity
type CreateLegalEntityEndpoint struct {
	Meta core.Pattern `method:"POST" path:"/v1/LegalEntity"`
	Body models.LegalEntity `body:"json"`

	RCService RCService
}

func (e *CreateLegalEntityEndpoint) Handle(ctx context.Context) (any, error) {
	entity, err := e.RCService.CreateLegalEntity(ctx, e.Body)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, entity), nil
}

// GetMyLegalEntitiesEndpoint handles GET /v1/LegalEntity/mine
type GetMyLegalEntitiesEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/LegalEntity/mine"`

	RCService RCService
}

func (e *GetMyLegalEntitiesEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	entities, err := e.RCService.GetMyLegalEntities(ctx, uid)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, err.Error()), nil
	}
	return response.New(http.StatusOK, entities), nil
}

// GetPOSEndpoint handles GET /v1/POS/{id}
type GetPOSEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/POS/{id}"`
	Id   int64        `path:"id"`

	RCService RCService
}

func (e *GetPOSEndpoint) Handle(ctx context.Context) (any, error) {
	pos, err := e.RCService.GetPOS(ctx, e.Id)
	if err != nil {
		return response.Error[any](http.StatusNotFound, err.Error()), nil
	}
	return response.New(http.StatusOK, pos), nil
}

// CreatePOSEndpoint handles POST /v1/POS
type CreatePOSEndpoint struct {
	Meta core.Pattern `method:"POST" path:"/v1/POS"`
	Body models.POS   `body:"json"`

	RCService RCService
}

func (e *CreatePOSEndpoint) Handle(ctx context.Context) (any, error) {
	pos, err := e.RCService.CreatePOS(ctx, e.Body)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, pos), nil
}

// GetMyPOSesEndpoint handles GET /v1/POS/mine
type GetMyPOSesEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/POS/mine"`

	RCService RCService
}

func (e *GetMyPOSesEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	poses, err := e.RCService.GetMyPOSes(ctx, uid)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, err.Error()), nil
	}
	return response.New(http.StatusOK, poses), nil
}

// GetAvailableCoursesEndpoint handles GET /v1/Course/available
type GetAvailableCoursesEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/Course/available"`

	RCService RCService
}

func (e *GetAvailableCoursesEndpoint) Handle(ctx context.Context) (any, error) {
	courses, err := e.RCService.GetAvailableCourses(ctx)
	if err != nil {
		return response.Error[any](http.StatusInternalServerError, err.Error()), nil
	}
	return response.New(http.StatusOK, courses), nil
}

// GetCourseDetailsEndpoint handles GET /v1/Course/{id}
type GetCourseDetailsEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/Course/{id}"`
	Id   int16        `path:"id"`

	RCService RCService
}

func (e *GetCourseDetailsEndpoint) Handle(ctx context.Context) (any, error) {
	course, err := e.RCService.GetCourse(ctx, e.Id)
	if err != nil {
		return response.Error[any](http.StatusNotFound, err.Error()), nil
	}
	return response.New(http.StatusOK, course), nil
}

// GetMeEndpoint handles GET /v1/User
type GetMeEndpoint struct {
	Meta core.Pattern `method:"GET" path:"/v1/User"`

	RCService RCService
}

func (e *GetMeEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	user, err := e.RCService.GetMe(ctx, uid)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, user), nil
}

// UpdateMeEndpoint handles PATCH /v1/User
type UpdateMeEndpoint struct {
	Meta core.Pattern `method:"PATCH" path:"/v1/User"`
	Body models.User  `body:"json"`

	RCService RCService
}

func (e *UpdateMeEndpoint) Handle(ctx context.Context) (any, error) {
	uid, _ := ctx.Value("fUserId").(string)
	e.Body.ExternalId = uid
	err := e.RCService.UpdateMe(ctx, e.Body)
	if err != nil {
		return response.Error[any](http.StatusBadRequest, err.Error()), nil
	}
	return response.New(http.StatusOK, "updated"), nil
}
