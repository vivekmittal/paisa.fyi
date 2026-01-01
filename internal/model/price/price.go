package price

import (
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/ananthakumaran/paisa/internal/config"
	"github.com/google/btree"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

type Price struct {
	ID            uint                 `gorm:"primaryKey" json:"id"`
	Date          time.Time            `json:"date"`
	CommodityType config.CommodityType `json:"commodity_type"`
	CommodityID   string               `json:"commodity_id"`
	CommodityName string               `json:"commodity_name"`
	Value         decimal.Decimal      `json:"value"`
}

type UpsertResult struct {
	CommodityType config.CommodityType
	Name          string
	Code          string
	Prices        []*Price
}

func (p Price) Less(o btree.Item) bool {
	return p.Date.Before(o.(Price).Date)
}

func DeleteAll(db *gorm.DB) error {
	err := db.Exec("DELETE FROM prices").Error
	if err != nil {
		return err
	}
	return nil
}

func UpsertAllResults(db *gorm.DB, results []UpsertResult) error {
	return db.Transaction(func(tx *gorm.DB) error {
		// Collect all deletion conditions
		if len(results) > 0 {
			var types []config.CommodityType
			var names []string
			var codes []string

			for _, result := range results {
				types = append(types, result.CommodityType)
				names = append(names, result.Name)
				codes = append(codes, result.Code)
			}

			// Delete all matching records in one query
			err := tx.Where("commodity_type IN ? AND commodity_name IN ? AND commodity_id IN ?",
				types, names, codes).Delete(&Price{}).Error
			if err != nil {
				return fmt.Errorf("failed to delete prices: %w", err)
			}
		}

		// Collect all prices to insert
		var allPrices []*Price
		for _, result := range results {
			allPrices = append(allPrices, result.Prices...)
		}

		// Single bulk insert
		if len(allPrices) > 0 {
			// For very large datasets, you might still want batching here
			batchSize := 5000
			err := tx.CreateInBatches(allPrices, batchSize).Error
			if err != nil {
				return fmt.Errorf("failed to insert prices: %w", err)
			}
		}

		return nil
	})
}

func UpsertAllByType(db *gorm.DB, commodityType config.CommodityType, prices []Price) {
	err := db.Transaction(func(tx *gorm.DB) error {
		err := tx.Delete(&Price{}, "commodity_type = ?", commodityType).Error
		if err != nil {
			return err
		}

		if len(prices) > 0 {
			err := tx.CreateInBatches(prices, 100000).Error
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}
