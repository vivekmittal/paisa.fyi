package model

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ananthakumaran/paisa/internal/config"
	"github.com/ananthakumaran/paisa/internal/ledger"
	"github.com/ananthakumaran/paisa/internal/model/cache"
	"github.com/ananthakumaran/paisa/internal/model/cii"
	"github.com/ananthakumaran/paisa/internal/model/commodity"
	mutualfundModel "github.com/ananthakumaran/paisa/internal/model/mutualfund/scheme"
	npsModel "github.com/ananthakumaran/paisa/internal/model/nps/scheme"
	"github.com/ananthakumaran/paisa/internal/model/portfolio"
	"github.com/ananthakumaran/paisa/internal/model/posting"
	"github.com/ananthakumaran/paisa/internal/model/price"
	"github.com/ananthakumaran/paisa/internal/scraper"
	"github.com/ananthakumaran/paisa/internal/scraper/india"
	"github.com/ananthakumaran/paisa/internal/scraper/mutualfund"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

func AutoMigrate(db *gorm.DB) {
	db.AutoMigrate(&npsModel.Scheme{})
	db.AutoMigrate(&mutualfundModel.Scheme{})
	db.AutoMigrate(&posting.Posting{})
	db.AutoMigrate(&price.Price{})
	db.AutoMigrate(&portfolio.Portfolio{})
	db.AutoMigrate(&price.Price{})
	db.AutoMigrate(&cii.CII{})
	db.AutoMigrate(&cache.Cache{})
}

func SyncJournal(db *gorm.DB) (string, error) {
	AutoMigrate(db)
	log.Info("Syncing transactions from journal")

	errors, _, err := ledger.Cli().ValidateFile(config.GetJournalPath())
	if err != nil {

		if len(errors) == 0 {
			return err.Error(), err
		}

		var message string
		for _, error := range errors {
			message += error.Message + "\n\n"
		}
		return strings.TrimRight(message, "\n"), err
	}

	prices, err := ledger.Cli().Prices(config.GetJournalPath())
	if err != nil {
		return err.Error(), err
	}

	price.UpsertAllByType(db, config.Unknown, prices)

	postings, err := ledger.Cli().Parse(config.GetJournalPath(), prices)
	if err != nil {
		return err.Error(), err
	}
	posting.UpsertAll(db, postings)

	return "", nil
}

func SyncCommodities(db *gorm.DB) error {
	// Optimize for bulk writes
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")
	db.Exec("PRAGMA cache_size=-256000") // 128MB cache
	db.Exec("PRAGMA temp_store=MEMORY")

	AutoMigrate(db)
	log.Info("Fetching commodities price history")
	commodities := lo.Shuffle(commodity.All())

	// Start timing for fetching all securities
	fetchStart := time.Now()

	results := make(chan price.UpsertResult, len(commodities))
	wg := sync.WaitGroup{}

	for _, commodity := range commodities {
		name := commodity.Name
		log.Info("Fetching commodity ", name)

		wg.Add(1)
		go func(c config.Commodity) {
			defer wg.Done()

			name := c.Name
			code := c.Price.Code
			log.Info("Fetching commodity ", name)

			provider := scraper.GetProviderByCode(c.Price.Provider)
			prices, err := provider.GetPrices(code, name)

			if err != nil {
				log.Error(err)
			}

			results <- price.UpsertResult{
				CommodityType: c.Type,
				Name:          name,
				Code:          code,
				Prices:        prices,
			}
		}(commodity)
	}

	// Wait for all fetches to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect all results and measure fetch time
	var allResults []price.UpsertResult
	for r := range results {
		allResults = append(allResults, r)
	}

	fetchDuration := time.Since(fetchStart)
	log.Infof("Fetched all securities in %v", fetchDuration)

	// Single database transaction for all writes
	if len(allResults) > 0 {
		// Start timing for database upsert
		upsertStart := time.Now()
		if err := price.UpsertAllResults(db, allResults); err != nil {
			return fmt.Errorf("database transaction failed: %w", err)
		}
		upsertDuration := time.Since(upsertStart)
		log.Infof("Upserted all securities in database in %v", upsertDuration)
	}

	return nil
}

func SyncCII(db *gorm.DB) error {
	AutoMigrate(db)
	log.Info("Fetching taxation related info")
	ciis, err := india.GetCostInflationIndex()
	if err != nil {
		log.Error(err)
		return fmt.Errorf("Failed to fetch CII: %w", err)
	}
	cii.UpsertAll(db, ciis)
	return nil
}

func SyncPortfolios(db *gorm.DB) error {
	db.AutoMigrate(&portfolio.Portfolio{})
	log.Info("Fetching commodities portfolio")
	commodities := commodity.FindByType(config.MutualFund)
	for _, commodity := range commodities {
		if commodity.Price.Provider != "in-mfapi" {
			continue
		}

		name := commodity.Name
		log.Info("Fetching portfolio for ", name)
		portfolios, err := mutualfund.GetPortfolio(commodity.Price.Code, commodity.Name)

		if err != nil {
			log.Error(err)
			return fmt.Errorf("Failed to fetch portfolio for %s: %w", name, err)
		}

		portfolio.UpsertAll(db, commodity.Type, commodity.Price.Code, portfolios)
	}
	return nil
}
