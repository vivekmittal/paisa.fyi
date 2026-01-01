package server

import (
	"sync"
	"time"

	"github.com/ananthakumaran/paisa/internal/query"
	"github.com/ananthakumaran/paisa/internal/server/assets"
	"github.com/ananthakumaran/paisa/internal/server/goal"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// runWithLogging executes a function in a goroutine with logging and time tracking.
// It logs when the operation starts and finishes, including the duration.
func runWithLogging(wg *sync.WaitGroup, operation string, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Infof("Starting operation: %s", operation)
		start := time.Now()
		fn()
		duration := time.Since(start)
		log.Infof("Finished operation: %s (took %v)", operation, duration)
	}()
}

func GetDashboard(db *gorm.DB) gin.H {
	var (
		checkingBalances     interface{}
		networth             interface{}
		expenses             interface{}
		cashFlows            interface{}
		transactionSequences interface{}
		transactions         interface{}
		budget               interface{}
		goalSummaries        interface{}
	)

	var wg sync.WaitGroup

	runWithLogging(&wg, "GetCheckingBalance", func() {
		checkingBalances = assets.GetCheckingBalance(db)
	})

	runWithLogging(&wg, "GetCurrentNetworth", func() {
		networth = GetCurrentNetworth(db)
	})

	runWithLogging(&wg, "GetCurrentExpense", func() {
		expenses = GetCurrentExpense(db)
	})

	runWithLogging(&wg, "GetCurrentCashFlow", func() {
		cashFlows = GetCurrentCashFlow(db)
	})

	runWithLogging(&wg, "ComputeRecurringTransactions", func() {
		transactionSequences = ComputeRecurringTransactions(query.Init(db).All())
	})

	runWithLogging(&wg, "GetLatestTransactions", func() {
		transactions = GetLatestTransactions(db)
	})

	runWithLogging(&wg, "GetCurrentBudget", func() {
		budget = GetCurrentBudget(db)
	})

	runWithLogging(&wg, "GetGoalSummaries", func() {
		goalSummaries = goal.GetGoalSummaries(db)
	})

	wg.Wait()

	return gin.H{
		"checkingBalances":     checkingBalances,
		"networth":             networth,
		"expenses":             expenses,
		"cashFlows":            cashFlows,
		"transactionSequences": transactionSequences,
		"transactions":         transactions,
		"budget":               budget,
		"goalSummaries":        goalSummaries,
	}
}
