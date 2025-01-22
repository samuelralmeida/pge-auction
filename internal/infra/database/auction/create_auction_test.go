package auction_test

import (
	"context"
	"testing"
	"time"

	"github.com/benweissmann/memongo"

	"github.com/samuelralmeida/pge-auction/internal/entity/auction_entity"
	"github.com/samuelralmeida/pge-auction/internal/infra/database/auction"
	"github.com/samuelralmeida/pge-auction/internal/internal_error"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestAuctionRepository_expireAuctions_markAsCompleted(t *testing.T) {
	ctx := context.Background()

	server, err := memongo.Start("4.0.5")
	if err != nil {
		t.Error("error to start mongo in memory")
		return
	}
	defer server.Stop()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(server.URI()))
	if err != nil {
		t.Error("error to connect mongo in memory")
	}

	database := client.Database("auctions-test")

	repo := auction.NewAuctionRepository(database)

	err = repo.CreateAuction(ctx, &auction_entity.Auction{
		Id:          "expired-auction-id",
		ProductName: "expired-auction",
		Category:    "some-category",
		Description: "some-description",
		Condition:   auction_entity.New,
		Status:      auction_entity.Active,
		Timestamp:   time.Now().Add(-time.Hour),
	})

	// o código do desafio retorna um ponteiro de um erro customizado.
	// para fazer a verificação de erro foi preciso fazer "type assertion"
	// caso contrário estava dando "tautological condition: non-nil != nil (nilness)"
	errResp := err.(*internal_error.InternalError)
	if errResp != nil {
		t.Errorf("error to create auction: %v - %T\n", err, err)
		return
	}

	repo.ExpireAuctions(ctx)

	updatedAuction, err := repo.FindAuctionById(ctx, "expired-auction-id")
	errResp = err.(*internal_error.InternalError)
	if errResp != nil {
		t.Errorf("error to find auction: %v\n", err)
		return
	}

	if updatedAuction.Status != auction_entity.Completed {
		t.Errorf("Expected auction to be closed, got %v", updatedAuction.Status)
	}
}

func TestAuctionRepository_expireAuctions_notMarkAsCompleted(t *testing.T) {
	ctx := context.Background()

	server, err := memongo.Start("4.0.5")
	if err != nil {
		t.Error("error to start mongo in memory")
		return
	}
	defer server.Stop()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(server.URI()))
	if err != nil {
		t.Error("error to connect mongo in memory")
	}

	database := client.Database("auctions-test")

	repo := auction.NewAuctionRepository(database)

	err = repo.CreateAuction(ctx, &auction_entity.Auction{
		Id:          "expired-auction-id",
		ProductName: "expired-auction",
		Category:    "some-category",
		Description: "some-description",
		Condition:   auction_entity.New,
		Status:      auction_entity.Active,
		Timestamp:   time.Now().Add(+time.Hour),
	})

	// o código do desafio retorna um ponteiro de um erro customizado.
	// para fazer a verificação de erro foi preciso fazer "type assertion"
	// caso contrário estava dando "tautological condition: non-nil != nil (nilness)"
	errResp := err.(*internal_error.InternalError)
	if errResp != nil {
		t.Errorf("error to create auction: %v - %T\n", err, err)
		return
	}

	repo.ExpireAuctions(ctx)

	updatedAuction, err := repo.FindAuctionById(ctx, "expired-auction-id")
	errResp = err.(*internal_error.InternalError)
	if errResp != nil {
		t.Errorf("error to find auction: %v\n", err)
		return
	}

	if updatedAuction.Status != auction_entity.Active {
		t.Errorf("Expected auction to be closed, got %v", updatedAuction.Status)
	}
}
