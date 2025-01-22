package auction

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/samuelralmeida/pge-auction/configuration/logger"
	"github.com/samuelralmeida/pge-auction/internal/entity/auction_entity"
	"github.com/samuelralmeida/pge-auction/internal/internal_error"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type AuctionEntityMongo struct {
	Id          string                          `bson:"_id"`
	ProductName string                          `bson:"product_name"`
	Category    string                          `bson:"category"`
	Description string                          `bson:"description"`
	Condition   auction_entity.ProductCondition `bson:"condition"`
	Status      auction_entity.AuctionStatus    `bson:"status"`
	Timestamp   int64                           `bson:"timestamp"`
}
type AuctionRepository struct {
	Collection *mongo.Collection
}

func NewAuctionRepository(database *mongo.Database) *AuctionRepository {
	return &AuctionRepository{
		Collection: database.Collection("auctions"),
	}
}

func (ar *AuctionRepository) CreateAuction(
	ctx context.Context,
	auctionEntity *auction_entity.Auction) *internal_error.InternalError {
	auctionEntityMongo := &AuctionEntityMongo{
		Id:          auctionEntity.Id,
		ProductName: auctionEntity.ProductName,
		Category:    auctionEntity.Category,
		Description: auctionEntity.Description,
		Condition:   auctionEntity.Condition,
		Status:      auctionEntity.Status,
		Timestamp:   auctionEntity.Timestamp.Unix(),
	}
	_, err := ar.Collection.InsertOne(ctx, auctionEntityMongo)
	if err != nil {
		logger.Error("Error trying to insert auction", err)
		return internal_error.NewInternalServerError("Error trying to insert auction")
	}

	return nil
}

func (ar *AuctionRepository) StartAuctionExpirationWatcher(ctx context.Context) {
	ticker := time.NewTicker(getAuctionExpirationWatcherMinutes() * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ar.ExpireAuctions(ctx)
		case <-ctx.Done():
			logger.Info("Auction expiration watcher stopped")
			return
		}
	}
}

func (ar *AuctionRepository) ExpireAuctions(ctx context.Context) {
	now := time.Now().Unix()
	filter := bson.M{
		"status":    auction_entity.Active,
		"timestamp": bson.M{"$lt": now},
	}
	update := bson.M{"$set": bson.M{"status": auction_entity.Completed}}

	result, err := ar.Collection.UpdateMany(ctx, filter, update)
	if err != nil {
		logger.Error("Error updating expired auctions", err)
	} else {
		logger.Info(fmt.Sprintf("Closed %d expired auctions", result.ModifiedCount))
	}
}

func getAuctionExpirationWatcherMinutes() time.Duration {
	value, err := strconv.Atoi(os.Getenv("AUCTION_EXPIRATION_WATCHER_SECONDS"))
	if err != nil {
		return 1
	}

	return time.Duration(value)
}
