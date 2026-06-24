package classify

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/rs/zerolog/log"
)

type Runner struct {
	classifier *Classifier
	results    *messaging.Publisher
}

func NewRunner(classifier *Classifier, results *messaging.Publisher) *Runner {
	return &Runner{classifier: classifier, results: results}
}

func (r *Runner) Handle(ctx context.Context, msg messaging.Message) error {
	var req Request
	if err := json.Unmarshal(msg.Value, &req); err != nil {
		log.Error().Err(err).Msg("invalid classify Request; dropping")
		return nil
	}

	addrType, err := r.classifier.Classify(ctx, req.Chain, req.Address)
	if err != nil {
		return nil
	}

	res := Result{
		Chain:        req.Chain,
		Address:      req.Address,
		AddressType:  addrType,
		ClassifiedAt: time.Now(),
	}

	if err := r.results.PublishJSON(ctx, req.Address, res); err != nil {
		return err
	}

	log.Debug().Str("address", req.Address).Str("type", addrType).Msg("classified")
	return nil
}
